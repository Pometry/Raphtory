"""End-to-end tests for the Transport-based RemoteGraph API.

Exercises the full stack: Python API → PyO3 wrapper → Rust RemoteGraph →
Op::Write/Op::Read → GraphqlTransport → GraphQL server → response back.

All writes and the .degree() read go through Transport::execute; the .window()
and .node() calls are lazy expression builders that fire the RPC only on the
terminal.
"""

import tempfile

from raphtory.graphql import GraphServer


def _make_graph_with_edge():
    """Set up a graph with two nodes and an edge at t=3.

    Returns the running-server context manager and the RemoteGraph handle;
    caller keeps the server alive with `with`.
    """
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    client = server.get_client()
    client.new_graph("test-graph", "EVENT")
    rg = client.remote_graph("test-graph")
    rg.add_node(1, "ben")
    rg.add_node(2, "hamza")
    rg.add_edge(3, "ben", "hamza")
    return server_cm, rg


def test_add_and_degree():
    """Writes and unwindowed reads both route through Transport."""
    server_cm, rg = _make_graph_with_edge()
    try:
        assert rg.node("ben").degree() == 1
        assert rg.node("hamza").degree() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_windowed_degree():
    """`.window()` composes with `.node().degree()` — RPC is fired only at `.degree()`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Window [0, 5) includes the edge added at t=3.
        assert rg.window(0, 5).node("ben").degree() == 1
        # Window [0, 2) excludes the edge — ben has no in-window neighbours.
        assert rg.window(0, 2).node("ben").degree() == 0
    finally:
        server_cm.__exit__(None, None, None)


def test_view_chain_propagation():
    """`PyRemoteGraph.node()` must forward the accumulated view chain into the
    returned `RemoteNode` — otherwise the window is silently dropped and both
    windowed queries collapse to the global degree.
    """
    server_cm, rg = _make_graph_with_edge()
    try:
        d_including_edge = rg.window(0, 5).node("ben").degree()
        d_excluding_edge = rg.window(0, 2).node("ben").degree()
        assert d_including_edge != d_excluding_edge, (
            "windowed queries should differ — if they don't, the view chain is "
            "being dropped when descending from RemoteGraph to RemoteNode"
        )
    finally:
        server_cm.__exit__(None, None, None)
