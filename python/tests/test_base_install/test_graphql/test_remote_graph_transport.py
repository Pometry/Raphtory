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


def test_graph_terminals():
    """`count_nodes` / `count_edges` on `RemoteGraph`, both unwindowed and
    under a view chain."""
    server_cm, rg = _make_graph_with_edge()
    try:
        assert rg.count_nodes() == 2
        assert rg.count_edges() == 1

        # Window [0, 3) includes ben (t=1) and hamza (t=2) but excludes the
        # edge (added at t=3, and window end is exclusive).
        rg_narrow = rg.window(0, 3)
        assert rg_narrow.count_nodes() == 2
        assert rg_narrow.count_edges() == 0
    finally:
        server_cm.__exit__(None, None, None)


def test_node_terminals():
    """`.name()`, `.in_degree()`, `.out_degree()` on `RemoteNode`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        assert ben.name() == "ben"
        assert ben.out_degree() == 1  # ben → hamza
        assert ben.in_degree() == 0

        hamza = rg.node("hamza")
        assert hamza.out_degree() == 0
        assert hamza.in_degree() == 1  # ben → hamza
    finally:
        server_cm.__exit__(None, None, None)


def test_view_ops():
    """`.at(...)`, `.before(...)`, `.after(...)` are lazy builders that
    compose with terminals. Server-side `.after` is an exclusive lower bound
    (strictly-after semantics), `.before` is an exclusive upper bound."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # `.before(3)` — strictly before t=3 — edge at t=3 not visible.
        assert rg.before(3).node("ben").degree() == 0
        # `.before(4)` — includes the edge at t=3.
        assert rg.before(4).node("ben").degree() == 1
        # `.after(0)` — strictly after t=0 — all events visible.
        assert rg.after(0).node("ben").degree() == 1
        # `.at(3)` snapshots at t=3 — edge exists.
        assert rg.at(3).node("ben").degree() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_selection_and_navigation():
    """`rg.edge(src, dst)` selects an edge; `.src()` / `.dst()` navigate back
    to node handles that carry the whole view chain."""
    server_cm, rg = _make_graph_with_edge()
    try:
        e = rg.edge("ben", "hamza")
        # Navigate back to source/destination nodes and read from them.
        assert e.src().name() == "ben"
        assert e.dst().name() == "hamza"
        # The navigated-back node handles carry the full view chain — evaluating
        # a terminal on them fires an RPC against the same underlying edge.
        assert e.src().degree() == 1
        assert e.dst().degree() == 1
    finally:
        server_cm.__exit__(None, None, None)
