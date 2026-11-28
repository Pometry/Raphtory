#!/usr/bin/env bash
# Smoke test for a published Raphtory Docker image.
#
# Starts the image, then verifies that:
#   1. the process actually starts and stays up (catches, e.g., a GLIBC
#      mismatch between the build and runtime stages),
#   2. the health endpoint reports healthy,
#   3. the version endpoint responds,
#   4. the UI is served,
#   5. a basic GraphQL query executes.
#
# Usage:
#   docker-smoke-test.sh <image-ref> [container-args...]
#
#   <image-ref>       e.g. "pometry/raphtory:0.18.0" or "pometry/raphtory@sha256:…"
#   [container-args]  passed to the container, e.g. "server" for the Rust image
#                     (the Python image's entrypoint already includes it)
#
# Environment:
#   HOST_PORT  Host port to bind (default 1736).
#   TIMEOUT    Seconds to wait for the server to become healthy (default 60).

set -euo pipefail

IMAGE_REF="${1:?usage: $0 <image-ref> [container-args...]}"
shift
HOST_PORT="${HOST_PORT:-1736}"
TIMEOUT="${TIMEOUT:-60}"
CONTAINER_NAME="raphtory-smoke-$$"

log() { printf 'smoke-test: %s\n' "$*" >&2; }

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! docker image inspect "$IMAGE_REF" >/dev/null 2>&1; then
  log "pulling image $IMAGE_REF"
  docker pull "$IMAGE_REF"
fi

log "starting container (host port $HOST_PORT)"
docker run -d \
  --name "$CONTAINER_NAME" \
  -p "127.0.0.1:${HOST_PORT}:1736" \
  "$IMAGE_REF" "$@"

healthy=0
for _ in $(seq 1 "$TIMEOUT"); do
  # If the container died (e.g. missing GLIBC symbols), fail fast with its logs.
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    log "container exited before becoming healthy; logs:"
    docker logs "$CONTAINER_NAME" >&2 || true
    exit 1
  fi
  if curl -fsS "http://127.0.0.1:${HOST_PORT}/health" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done

if [ "$healthy" -ne 1 ]; then
  log "server did not become healthy within ${TIMEOUT}s; logs:"
  docker logs "$CONTAINER_NAME" >&2 || true
  exit 1
fi

log "checking /health"
health_body="$(curl -fsS "http://127.0.0.1:${HOST_PORT}/health")"
if ! printf '%s' "$health_body" | grep -q '"healthy"[[:space:]]*:[[:space:]]*true'; then
  log "unexpected /health response: $health_body"
  exit 1
fi

log "checking /version"
curl -fsS "http://127.0.0.1:${HOST_PORT}/version" >/dev/null

log "checking UI (GET /)"
curl -fsS "http://127.0.0.1:${HOST_PORT}/" >/dev/null

log "checking GraphQL query (POST /)"
gql_body="$(curl -fsS \
  -H 'content-type: application/json' \
  -d '{"query":"{__typename}"}' \
  "http://127.0.0.1:${HOST_PORT}/")"
if ! printf '%s' "$gql_body" | grep -q '__typename'; then
  log "unexpected GraphQL response: $gql_body"
  exit 1
fi

log "OK"
