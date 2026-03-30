// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::time::Duration;

use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use http::{Method, Request, Response};

use restate_partition_store::{StateChangeEvent, StateChangeOperation};
use restate_types::identifiers::ServiceId;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;

use super::HandlerError;
use super::path_parsing::ObjectStateRequestType;
use crate::RequestDispatcher;

// SSE uses `Last-Event-ID` header (not a standard http::header constant)
const LAST_EVENT_ID: http::header::HeaderName =
    http::header::HeaderName::from_static("last-event-id");

impl<Schemas, Dispatcher> super::Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_object_state<B>(
        &mut self,
        req: Request<B>,
        object_request: ObjectStateRequestType,
    ) -> Result<Response<axum::body::Body>, HandlerError>
    where
        B: http_body::Body + Send + 'static,
        <B as http_body::Body>::Data: Send + 'static,
        <B as http_body::Body>::Error: Into<restate_types::errors::GenericError>,
    {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let accept = req.headers().get(http::header::ACCEPT);
        if !accept
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream") || v.contains("*/*"))
            .unwrap_or(true)
        {
            return Err(HandlerError::NotAcceptable("text/event-stream".to_owned()));
        }

        let ObjectStateRequestType { service, key } = object_request;

        match self.schemas.pinned().resolve_latest_service(&service) {
            None => return Err(HandlerError::ServiceNotFound(service)),
            Some(svc) if !svc.ty.has_state() => {
                return Err(HandlerError::ServiceHasNoState(service));
            }
            Some(_) => {}
        }

        let service_id = ServiceId::new(None, service, key);

        let router = self
            .state_router
            .as_ref()
            .ok_or(HandlerError::Unavailable)?
            .clone();

        let last_event_id: Option<u64> = req
            .headers()
            .get(&LAST_EVENT_ID)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());

        let (snapshot, rx) = router
            .subscribe(
                tokio::time::Instant::now(),
                service_id.clone(),
                last_event_id,
            )
            .await
            .map_err(|_| HandlerError::Unavailable)?;

        let stream = futures::stream::unfold((snapshot, rx), |(snapshot, mut rx)| async move {
            if let Some(event) = snapshot {
                return Some((
                    Ok::<Event, Infallible>(router_event_to_sse(&event)),
                    (None, rx),
                ));
            }
            match rx.recv().await {
                Ok(event) => Some((Ok(router_event_to_sse(&event)), (None, rx))),
                // Lagged (slow client) or channel closed → end stream; browser reconnects.
                Err(_) => None,
            }
        });

        Ok(Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response())
    }
}

fn router_event_to_sse(event: &StateChangeEvent) -> Event {
    match &event.operation {
        StateChangeOperation::Replace { state } => {
            let map: serde_json::Map<_, _> = state
                .iter()
                .map(|(k, v)| (k.clone(), StateChangeOperation::encode_value(v)))
                .collect();
            Event::default()
                .data(format!("RPL {}", serde_json::Value::Object(map)))
                .id(event.revision.to_string())
                .retry(Duration::from_millis(3000))
        }
        StateChangeOperation::Patch { assigned, deleted } => {
            let mut parts: Vec<String> = Vec::new();

            if !assigned.is_empty() {
                let map: serde_json::Map<_, _> = assigned
                    .iter()
                    .map(|(k, v)| (k.clone(), StateChangeOperation::encode_value(v)))
                    .collect();
                parts.push(format!("ASN {}", serde_json::Value::Object(map)));
            }

            if !deleted.is_empty() {
                let keys: Vec<_> = deleted
                    .iter()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .collect();
                parts.push(format!("DEL {}", serde_json::Value::Array(keys)));
            }

            // axum's Event::data may only be called once; join multiple payload
            // lines with '\n' — axum splits on newlines into separate SSE data: lines.
            Event::default()
                .id(event.revision.to_string())
                .data(parts.join("\n"))
        }
        StateChangeOperation::ClearAll => {
            Event::default().data("CLR").id(event.revision.to_string())
        }
    }
}
