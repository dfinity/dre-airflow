FROM docker.io/rust:1.80-bookworm as rust
WORKDIR /usr/src
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/src ./src/
RUN cargo install --path .

FROM docker.io/node:alpine as node
WORKDIR /
COPY frontend/index.html frontend/*.json frontend/*.js frontend/*.ts ./
COPY frontend/public ./public
COPY frontend/src ./src
RUN npm install && npm run build

FROM docker.io/debian:bookworm
WORKDIR /
RUN apt-get update && apt install -y openssl ca-certificates
COPY --from=rust /usr/local/cargo/bin/rollout-dashboard /rollout-dashboard
COPY --from=node /dist /static

EXPOSE 4174
ENV FRONTEND_STATIC_DIR=static
CMD ["./rollout-dashboard"]
