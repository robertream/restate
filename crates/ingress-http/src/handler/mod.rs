// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod awakeables;
mod error;
mod health;
mod invocation;
mod lookup;
mod objects;
mod path_parsing;
mod responses;
mod service_handler;
#[cfg(test)]
mod tests;
mod tracing;
mod workflow;

use std::convert::Infallible;
use std::task::{Context, Poll};

use bytestring::ByteString;
use enumset::EnumSet;
use error::HandlerError;
use futures::FutureExt;
use futures::future::BoxFuture;
use hyper::http::HeaderValue;
use hyper::{Request, Response};
use serde::Deserialize;

use restate_core::Metadata;
use restate_types::Scope;
use restate_types::errors::GenericError;
use restate_types::identifiers::{IdempotencyId, ServiceId};
use restate_types::invocation::InvocationQuery;
use restate_types::live::Live;
use restate_types::nodes_config::ClusterFeature;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_util_string::{ReString, RestrictedValue};

use super::*;
use crate::handler::path_parsing::{
    AwakeableRequestType, InvocationRequestType, ObjectStateRequestType, ServiceRequestType,
    WorkflowRequestType,
};
use crate::state_router::StateRouter;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

enum RequestType {
    Health,
    OpenAPI,
    Awakeable(AwakeableRequestType),
    Invocation(InvocationRequestType),
    Service(ServiceRequestType),
    Workflow(WorkflowRequestType),
    /// `GET /restate/attach/{invocation_id}`
    Attach(InvocationId),
    /// `GET /restate/output/{invocation_id}`
    Output(InvocationId),
    /// `POST /restate/attach` with a body resolving to an invocation target
    AttachByTarget,
    /// `POST /restate/output` with a body resolving to an invocation target
    OutputByTarget,
    /// `POST /restate/lookup`
    Lookup,
    /// `GET /restate/objects/{service}/{key}/state`
    ObjectState(ObjectStateRequestType),
}

#[derive(Clone)]
pub(crate) struct Handler<Schemas, Dispatcher> {
    schemas: Live<Schemas>,
    dispatcher: Dispatcher,
    cluster_features: EnumSet<ClusterFeature>,
    state_router: Option<StateRouter>,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) fn new(
        schemas: Live<Schemas>,
        dispatcher: Dispatcher,
        state_router: Option<StateRouter>,
    ) -> Self {
        let cluster_features = Metadata::with_current(|m| m.nodes_config_ref().features());

        Self {
            schemas,
            dispatcher,
            cluster_features,
            state_router,
        }
    }
}

impl<Schemas, Dispatcher, ReqBody> tower::Service<Request<ReqBody>> for Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
    ReqBody: http_body::Body + Send + 'static,
    <ReqBody as http_body::Body>::Data: Send + 'static,
    <ReqBody as http_body::Body>::Error: Into<GenericError>,
{
    type Response = Response<axum::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let res = self.parse_path(req.uri());

        let mut this = self.clone();
        async move {
            match res? {
                RequestType::Health => this
                    .handle_health(req)
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::OpenAPI => {
                    // TODO
                    Err(HandlerError::NotImplemented)
                }
                RequestType::Awakeable(awakeable_request) => this
                    .handle_awakeable(req, awakeable_request)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::Service(service_request) => this
                    .handle_service_request(req, service_request)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::Invocation(invocation_request) => this
                    .handle_invocation(req, invocation_request)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::Workflow(workflow_request) => this
                    .handle_workflow(req, workflow_request)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::ObjectState(object_request) => {
                    this.handle_object_state(req, object_request).await
                }
                RequestType::Attach(invocation_id) => this
                    .handle_invocation_attach(req, InvocationQuery::Invocation(invocation_id))
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::Output(invocation_id) => this
                    .handle_invocation_get_output(
                        req,
                        InvocationQuery::Invocation(invocation_id),
                    )
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::AttachByTarget => this
                    .handle_attach_by_target(req)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::OutputByTarget => this
                    .handle_output_by_target(req)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
                RequestType::Lookup => this
                    .handle_lookup(req)
                    .await
                    .map(|resp| resp.map(axum::body::Body::new)),
            }
        }
        .map(|r| Ok::<_, Infallible>(r.unwrap_or_else(|e: HandlerError| e.into_response())))
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum InvocationTargetRequest {
    Workflow {
        name: ReString,
        key: ReString,
        #[serde(default)]
        scope: Option<ReString>,
    },
    Idempotency {
        service: ReString,
        #[serde(default)]
        service_key: Option<ReString>,
        handler: ReString,
        idempotency_key: ReString,
        #[serde(default)]
        scope: Option<ReString>,
    },
}

impl InvocationTargetRequest {
    pub(crate) fn into_invocation_query(self) -> Result<InvocationQuery, HandlerError> {
        let scope_value = match self {
            Self::Workflow { ref scope, .. } | Self::Idempotency { ref scope, .. } => scope.clone(),
        };

        // Unfortunately, we cannot first check the existence of the service/handler or workflow
        // because it might have been removed from the Schema after an invocation having completed :-(
        // For such a check to work, we need to keep information about previously registered services
        // and workflows. Hence, we can only validate that the scope value and avoid interning.
        let scope = match scope_value {
            None => None,
            Some(s) => Some(Scope::new(
                RestrictedValue::new(s).map_err(HandlerError::BadScopeValue)?,
            )),
        };

        Ok(match self {
            Self::Workflow { name, key, .. } => InvocationQuery::Workflow(ServiceId::new(
                scope,
                ByteString::from(name.as_str()),
                ByteString::from(key.as_str()),
            )),
            Self::Idempotency {
                service,
                service_key,
                handler,
                idempotency_key,
                ..
            } => InvocationQuery::IdempotencyId(IdempotencyId::new(
                ByteString::from(service.as_str()),
                service_key.map(|s| ByteString::from(s.as_str())),
                ByteString::from(handler.as_str()),
                ByteString::from(idempotency_key.as_str()),
                scope,
            )),
        })
    }
}
