"""Counting HTTP proxy: make the remote client's round trips observable.

The remote client's docstrings state an explicit transport contract — every
view op claims "Lazy — no RPC.", every terminal and write claims "Fires one
RPC." (with a handful of documented multi-RPC exceptions). No value-comparing
test can check those claims: a client that fired three round trips per
terminal would still answer correctly. The only place the contract is
observable is the wire, so this module counts requests there.

A threaded stdlib HTTP proxy sits between the client and the real
``GraphServer``: it forwards every request verbatim (method, body,
content-type) and returns the upstream response unchanged, incrementing a
thread-safe counter on each POST. Every GraphQL RPC is a POST to the server
URL; the client's constructor additionally issues one GET connectivity probe,
which is forwarded so the client connects but not counted — it is not an RPC.

No product code is involved: the client is simply constructed against the
proxy's URL instead of the server's.
"""

from __future__ import annotations

import contextlib
import http.server
import tempfile
import threading

import requests

from raphtory.graphql import GraphServer, RaphtoryClient

# Generous forward timeout: a hung upstream should fail one test, not the run.
_FORWARD_TIMEOUT_S = 60


class RpcCounter:
    """A thread-safe count of GraphQL RPCs (HTTP POSTs) seen by the proxy."""

    def __init__(self):
        self._lock = threading.Lock()
        self._count = 0

    @property
    def value(self):
        with self._lock:
            return self._count

    def reset(self):
        with self._lock:
            self._count = 0

    def _increment(self):
        with self._lock:
            self._count += 1


def _forwarding_handler(upstream_url, counter):
    """Build a handler class forwarding to ``upstream_url``, counting POSTs."""

    class Handler(http.server.BaseHTTPRequestHandler):
        # The client keeps connections alive between requests; announcing
        # HTTP/1.1 plus the accurate Content-Length below is what lets one
        # connection carry many requests through the proxy.
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # noqa: A002 — stdlib signature
            pass  # keep pytest output clean

        def _forward(self, method):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            headers = {
                key: self.headers[key]
                for key in ("Content-Type", "Authorization", "Accept")
                if key in self.headers
            }
            upstream = requests.request(
                method,
                upstream_url + self.path,
                data=body,
                headers=headers,
                timeout=_FORWARD_TIMEOUT_S,
            )
            # `requests` has already decoded any Content-Encoding, so the body
            # is re-measured here rather than echoing upstream's length.
            payload = upstream.content
            try:
                self.send_response(upstream.status_code)
                content_type = upstream.headers.get("Content-Type")
                if content_type:
                    self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                # The client hung up mid-response (its GET connectivity probe
                # does this). The request itself was already forwarded and
                # counted; a torn-down connection is not an error worth noise.
                self.close_connection = True

        def do_GET(self):
            # The client constructor's connectivity probe (and `is_online`)
            # arrive as GETs — forwarded so the client connects, not counted:
            # an RPC is a GraphQL POST.
            self._forward("GET")

        def do_POST(self):
            counter._increment()
            self._forward("POST")

    return Handler


@contextlib.contextmanager
def counting_remote_graph(build=None, graph_type="EVENT"):
    """Start a GraphServer behind a counting proxy; yield ``(remote, counter)``.

    The ``RemoteGraph``'s client is connected to the *proxy*, so every round
    trip it makes is observable on ``counter``. ``build`` (if given) seeds the
    graph through the proxy too; the counter is reset just before yielding, so
    callers start from 0. The proxy binds an ephemeral port on 127.0.0.1 and
    is shut down (threads joined) before the server context exits.
    """
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            counter = RpcCounter()
            upstream_url = f"http://127.0.0.1:{server.port()}"
            proxy = http.server.ThreadingHTTPServer(
                ("127.0.0.1", 0), _forwarding_handler(upstream_url, counter)
            )
            # Idle keep-alive connections must not block `server_close`.
            proxy.daemon_threads = True
            thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            thread.start()
            try:
                proxy_url = f"http://127.0.0.1:{proxy.server_address[1]}"
                client = RaphtoryClient(proxy_url)
                client.new_graph("g", graph_type)
                remote = client.remote_graph("g")
                if build is not None:
                    build(remote)
                counter.reset()
                yield remote, counter
            finally:
                proxy.shutdown()
                proxy.server_close()
                thread.join()
