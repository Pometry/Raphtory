"""
Runs the Raphtory server on the graphs in GRAPHS_DIR, and a small static server for the YASGUI pages
in ui/, until Ctrl+C.

    python scripts/serve.py GRAPHS_DIR [PORT] [UI_PORT]
"""

import functools
import http.server
import os
import sys

from raphtory.graphql import GraphServer

HERE = os.path.dirname(os.path.abspath(__file__))
UI_DIR = os.path.join(HERE, "..", "ui")


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


def main(graphs_dir, port, ui_port):
    graphs = sorted(
        name for name in os.listdir(graphs_dir) if os.path.isdir(os.path.join(graphs_dir, name))
    )
    handler = functools.partial(QuietHandler, directory=UI_DIR)
    with GraphServer(graphs_dir).start(port=port), http.server.ThreadingHTTPServer(
        ("127.0.0.1", ui_port), handler
    ) as ui:
        server = f"http://localhost:{port}"
        print(f"Raphtory UI and GraphQL:  {server}/")
        for graph in graphs:
            print(f"SPARQL endpoint:          {server}/sparql/{graph}   (as of a date: ?asof=2024-01-01)")
        print(f"YASGUI pages:             http://localhost:{ui_port}/?server={server}")
        print("Ctrl+C to stop", flush=True)
        try:
            ui.serve_forever()
        except KeyboardInterrupt:
            print("stopped")


if __name__ == "__main__":
    if not 2 <= len(sys.argv) <= 4:
        sys.exit(__doc__)
    main(
        sys.argv[1],
        int(sys.argv[2]) if len(sys.argv) > 2 else 1736,
        int(sys.argv[3]) if len(sys.argv) > 3 else 1737,
    )
