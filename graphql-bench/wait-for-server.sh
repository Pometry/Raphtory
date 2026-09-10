#!/bin/sh
# Blocks until the raphtory graphql server answers a query, then exits 0.
#
# This replaces a fixed `sleep` before k6: server.py builds a multi-million node graph on a cold
# checkout and nothing listens on the port until that finishes, which is far longer than any sleep
# worth hardcoding. k6's setup() calls fail() on the first refused connection, which aborts the
# whole run before any scenario starts -- so the bench has to be sure the server is up first.
set -eu

URL="${RAPHTORY_URL:-http://localhost:1736}"
TIMEOUT="${WAIT_FOR_SERVER_TIMEOUT:-1200}"
INTERVAL="${WAIT_FOR_SERVER_INTERVAL:-2}"

echo ">>> waiting up to ${TIMEOUT}s for ${URL}"
elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
	if curl -sf -o /dev/null -X POST -H 'Content-Type: application/json' \
		-d '{"query":"{__typename}"}' "$URL"; then
		echo ">>> server answered after ${elapsed}s"
		exit 0
	fi
	sleep "$INTERVAL"
	elapsed=$((elapsed + INTERVAL))
	# one line a minute, rather than one every couple of seconds
	if [ $((elapsed % 60)) -eq 0 ]; then
		echo ">>> still waiting for ${URL} (${elapsed}s elapsed)"
	fi
done

echo ">>> ${URL} did not answer within ${TIMEOUT}s; giving up" >&2
exit 1
