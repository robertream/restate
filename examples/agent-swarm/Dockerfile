# Agent Swarm Demo — all-in-one container
#
# Exposed ports:
#   5173 — Demo frontend (Vite)
#   8080 — Restate ingress (SSE state endpoint)
#   9070 — Restate admin UI/API
#
# Build:  docker build -t agent-swarm-demo -f examples/agent-swarm/Dockerfile .
# Run:    docker run -p 5173:5173 -p 8080:8080 -p 9070:9070 agent-swarm-demo

# ─── Stage 1: Build Restate server ───
FROM rust:1.93-bookworm AS rust-builder

# All build deps discovered from the project's dev-tools image:
# protoc for prost, clang/libclang for rocksdb bindgen, cmake for various native deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libprotobuf-dev cmake clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace config
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY .cargo/ .cargo/

# Copy all workspace members + proto definitions
COPY crates/ crates/
COPY server/ server/
COPY tools/ tools/
COPY cli/ cli/
COPY lite/ lite/
COPY benchmarks/ benchmarks/
COPY workspace-hack/ workspace-hack/
COPY service-protocol/ service-protocol/

# Build the server binary (disable LTO to reduce link-time memory usage)
ENV CARGO_PROFILE_RELEASE_LTO=false
RUN cargo build --release -p restate-server \
    && mv target/release/restate-server /usr/local/bin/restate-server

# ─── Stage 2: Node.js for demo services + frontend ───
FROM node:22-bookworm-slim AS node-builder

WORKDIR /app
COPY examples/agent-swarm/package.json examples/agent-swarm/package-lock.json ./
RUN npm ci

# Download LLM model (~800MB)
RUN npx node-llama-cpp pull --dir ./models "hf:ggml-org/gemma-3-4b-it-GGUF:Q4_K_M"

COPY examples/agent-swarm/ .

# ─── Stage 3: Runtime ───
FROM node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy Restate server binary
COPY --from=rust-builder /usr/local/bin/restate-server /usr/local/bin/restate-server

# Copy demo app
WORKDIR /app
COPY --from=node-builder /app ./

# Use default Restate ports inside the container
RUN sed -i 's/localhost:8180/localhost:8080/g' vite.config.ts \
    && sed -i 's/localhost:9170/localhost:9070/g' ui/index.html

EXPOSE 5173 8080 9070

COPY --from=node-builder /app/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
