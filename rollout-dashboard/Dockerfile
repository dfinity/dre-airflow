FROM docker.io/rust:1.88-bookworm as rust
WORKDIR /usr/src
RUN cargo new rollout-dashboard
COPY server/Cargo.toml server/Cargo.lock ./rollout-dashboard/
WORKDIR /usr/src/rollout-dashboard
RUN cargo build --release ; rm -rf src ; echo "This step is expected to fail, but it creates a layer that is cacheable." >&2
COPY server/src ./src/
RUN cargo build --release && \
    mv target/release/rollout-dashboard .

FROM docker.io/node:alpine as node
WORKDIR /workdir
# The following step copies only the package files so npm install can be cached.
COPY frontend/package*.json ./
RUN npm install
COPY frontend/*.json frontend/index.html frontend/*.js frontend/*.ts ./
COPY frontend/public ./public
COPY frontend/src ./src
RUN npm run build

FROM docker.io/debian:bookworm
WORKDIR /
COPY --from=rust /usr/src/rollout-dashboard/rollout-dashboard /rollout-dashboard
COPY --from=node /workdir/dist/client /workdir/static
RUN apt-get update && apt-get upgrade -y && apt install -y openssl ca-certificates

EXPOSE 4174
ENV FRONTEND_STATIC_DIR=static
CMD ["./rollout-dashboard"]
