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


def test_compound_time_terminals():
    """Compound terminals (`earliest_time`, `latest_time`, `start`, `end`) require
    2-step JSON navigation (`<field> { timestamp }`) and can return `None` when
    the view has no events."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Unwindowed: earliest is t=1 (ben added), latest is t=3 (edge).
        assert rg.earliest_time() == 1
        assert rg.latest_time() == 3

        # Windowed [1, 3): earliest=1, latest=2 (hamza added at t=2; edge excluded).
        rg_win = rg.window(1, 3)
        assert rg_win.earliest_time() == 1
        assert rg_win.latest_time() == 2
        # Window bounds also come back through the same compound path.
        assert rg_win.start() == 1
        assert rg_win.end() == 3

        # On a Node — earliest/latest reflect the node's own events under the view.
        ben = rg.node("ben")
        assert ben.earliest_time() == 1  # ben added at t=1
        assert ben.latest_time() == 3    # participated in edge at t=3
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_bool_and_i64_terminals():
    """`has_node`, `has_edge`, `count_temporal_edges` on `RemoteGraph`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        assert rg.has_node("ben") is True
        assert rg.has_node("unknown") is False
        assert rg.has_edge("ben", "hamza") is True
        assert rg.has_edge("hamza", "ben") is False  # edges are directed
        # 1 edge added once → 1 temporal edge event.
        assert rg.count_temporal_edges() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_node_id_type_and_state():
    """`id`, `node_type`, `is_active`, `edge_history_count` on `RemoteNode`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        assert ben.id() == "ben"
        assert ben.node_type() is None  # type not set

        # Set a node type via the write path, then re-read.
        ben.set_node_type("person")
        assert rg.node("ben").node_type() == "person"

        # Ben participates in the ben→hamza edge → 1 edge history event.
        assert rg.node("ben").edge_history_count() == 1
        # Under a window that excludes the edge, no edge events for ben.
        assert rg.window(0, 3).node("ben").edge_history_count() == 0

        # is_active: ben has an event at t=1 (add_node), so active under a
        # view that includes t=1.
        assert rg.node("ben").is_active() is True
    finally:
        server_cm.__exit__(None, None, None)


def test_view_ops_batch2():
    """`.snapshot_at()`, `.latest()`, `.exclude_layer()`, `.shrink_window()` etc.
    All lazy builders that compose with terminals."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # `.snapshot_at(3)` — snapshot at t=3, edge is visible.
        assert rg.snapshot_at(3).node("ben").degree() == 1
        # `.latest()` — latest state, edge visible.
        assert rg.latest().node("ben").degree() == 1
        # `.snapshot_latest()` — snapshot at latest time, edge visible.
        assert rg.snapshot_latest().node("ben").degree() == 1
        # `.exclude_layer("_default")` — the edge was added on the default layer,
        # so excluding it should remove the edge from view (ben degree = 0).
        assert rg.exclude_layer("_default").node("ben").degree() == 0
        # `.shrink_window` — first widen with window(0, 10), then shrink to [1, 3).
        # Shrunk view excludes edge at t=3.
        assert rg.window(0, 10).shrink_window(1, 3).node("ben").degree() == 0
        # `.shrink_end(3)` — after window(0, 10), narrow to end=3.
        assert rg.window(0, 10).shrink_end(3).node("ben").degree() == 0
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_string_terminals():
    """`.name()`, `.path()`, `.namespace()` on `RemoteGraph`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # We created the graph at path "test-graph" — the leaf name is "test-graph"
        # and the namespace is the empty root.
        assert rg.name() == "test-graph"
        assert rg.path() == "test-graph"
        # Namespace of a top-level graph — server returns some form of it; just
        # confirm it's a string and doesn't error.
        assert isinstance(rg.namespace(), str)
    finally:
        server_cm.__exit__(None, None, None)


def test_list_arg_view_ops():
    """List-arg view ops: `.layers(...)`, `.exclude_layers(...)`, `.subgraph(...)`,
    `.subgraph_node_types(...)`, `.exclude_nodes(...)`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # `.layers(["_default"])` — restrict to default layer (where our edge lives).
        assert rg.layers(["_default"]).node("ben").degree() == 1
        # `.exclude_layers(["_default"])` — exclude the layer containing the edge.
        assert rg.exclude_layers(["_default"]).node("ben").degree() == 0
        # `.subgraph(["ben"])` — restrict to just the ben node.
        assert rg.subgraph(["ben"]).count_nodes() == 1
        # `.exclude_nodes(["hamza"])` — leaves just ben.
        assert rg.exclude_nodes(["hamza"]).count_nodes() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_default_layer_and_valid():
    """`.default_layer()` and `.valid()` are parameterless view builders."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # `.default_layer()` restricts to the default layer — edge is on it.
        assert rg.default_layer().node("ben").degree() == 1
        # `.valid()` filters out invalid entities. On an event graph with only
        # add ops, this is a no-op — count matches unwindowed.
        assert rg.valid().count_nodes() == 2
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_collection():
    """`rg.nodes` accessor returns a `RemoteNodes` collection with `.ids()`,
    `.count()`, and `.list()` terminals."""
    server_cm, rg = _make_graph_with_edge()
    try:
        nodes = rg.nodes
        assert nodes.count() == 2
        assert sorted(nodes.ids()) == ["ben", "hamza"]

        # Materialize as RemoteNode handles, then read a scalar off each.
        remote_nodes = nodes.list()
        assert len(remote_nodes) == 2
        names = sorted(n.name() for n in remote_nodes)
        assert names == ["ben", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_view_chain_propagates_through_collection_list():
    """Regression: previously `rg.window(...).nodes.list()` rebased returned
    nodes at Root, causing view-dependent terminals to silently give wrong
    answers. After the base_graph fix, materialized nodes carry the parent
    view forward.
    """
    server_cm, rg = _make_graph_with_edge()
    try:
        # Add a second edge at t=8 so we can distinguish global vs windowed degree.
        rg.add_edge(8, "ben", "hamza")

        # ben has 2 edge events total (t=3 and t=8), but degree is 1 globally
        # (same edge). Under window [0, 5), only the t=3 edge is visible.
        # Under window [6, 10), only the t=8 edge is visible.
        # Both cases: degree == 1. To distinguish, use edge_history_count.

        # For the correctness check we want a terminal whose value differs
        # under different views. Use `.edge_history_count()` — number of
        # temporal edge events involving this node in the view.
        # Unwindowed: 2 events. Window [0, 5): 1 event. Window [6, 10): 1.
        ben_unwindowed = rg.node("ben")
        assert ben_unwindowed.edge_history_count() == 2

        # Iterate through the windowed collection — the returned nodes should
        # carry the window, so edge_history_count reflects the windowed view.
        windowed_counts = []
        for n in rg.window(0, 5).nodes:
            if n.name() == "ben":
                windowed_counts.append(n.edge_history_count())
        assert windowed_counts == [1], (
            f"expected edge_history_count == 1 under [0,5) window, got {windowed_counts}. "
            "If this is 2, the view chain isn't propagating through .list()."
        )

        # Also verify via out_neighbours navigation.
        for n in rg.window(0, 5).node("ben").out_neighbours:
            # hamza in [0, 5): only the t=3 edge — history count 1.
            assert n.edge_history_count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_native_iteration():
    """`for n in rg.nodes:` — no explicit `.list()` needed."""
    server_cm, rg = _make_graph_with_edge()
    try:
        names = sorted(n.name() for n in rg.nodes)
        assert names == ["ben", "hamza"]

        # Native iteration over a navigation collection.
        out_names = [n.name() for n in rg.node("ben").out_neighbours]
        assert out_names == ["hamza"]

        # Iterating twice is idempotent (each iter() call fetches fresh).
        first = [n.name() for n in rg.nodes]
        second = [n.name() for n in rg.nodes]
        assert sorted(first) == sorted(second)
    finally:
        server_cm.__exit__(None, None, None)


def test_node_neighbour_collections():
    """`.neighbours`, `.in_neighbours`, `.out_neighbours` on `RemoteNode`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        # ben has one out-neighbour (hamza) and zero in-neighbours.
        assert ben.out_neighbours.ids() == ["hamza"]
        assert ben.in_neighbours.ids() == []
        # `.neighbours` is directed union — includes hamza.
        assert ben.neighbours.ids() == ["hamza"]

        hamza = rg.node("hamza")
        assert hamza.in_neighbours.ids() == ["ben"]
        assert hamza.out_neighbours.ids() == []
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


def test_edges_collection():
    """`rg.edges` accessor returns a `RemoteEdges` collection with `.count()`
    and `.list()` terminals. Unlike nodes, edges have no `.ids()` — they're
    identified by `(src, dst)` pairs."""
    server_cm, rg = _make_graph_with_edge()
    try:
        edges = rg.edges
        assert edges.count() == 1

        # Materialize as RemoteEdge handles; navigate back to endpoints.
        remote_edges = edges.list()
        assert len(remote_edges) == 1
        pairs = sorted((e.src().name(), e.dst().name()) for e in remote_edges)
        assert pairs == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_native_iteration():
    """`for e in rg.edges:` yields `RemoteEdge` handles without an explicit
    `.list()` call."""
    server_cm, rg = _make_graph_with_edge()
    # Add a second edge so we can verify multi-edge iteration.
    rg.add_node(4, "sam")
    rg.add_edge(5, "ben", "sam")
    try:
        pairs = sorted((e.src().name(), e.dst().name()) for e in rg.edges)
        assert pairs == [("ben", "hamza"), ("ben", "sam")]

        # Native iteration over a node's out_edges collection.
        out_pairs = sorted(
            (e.src().name(), e.dst().name()) for e in rg.node("ben").out_edges
        )
        assert out_pairs == [("ben", "hamza"), ("ben", "sam")]
    finally:
        server_cm.__exit__(None, None, None)


def test_node_edge_collections():
    """`.edges`, `.in_edges`, `.out_edges` on `RemoteNode`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        # ben → hamza: ben has one out-edge, zero in-edges.
        assert ben.out_edges.count() == 1
        assert ben.in_edges.count() == 0
        assert ben.edges.count() == 1

        hamza = rg.node("hamza")
        assert hamza.in_edges.count() == 1
        assert hamza.out_edges.count() == 0
        assert hamza.edges.count() == 1

        # The single out-edge from ben goes to hamza.
        out_pairs = [(e.src().name(), e.dst().name()) for e in ben.out_edges.list()]
        assert out_pairs == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_metadata_timestamps():
    """`created`, `last_opened`, `last_updated` on the graph return non-null
    system timestamps (wall-clock ms, set by the server when the graph is
    saved/opened/updated on disk)."""
    server_cm, rg = _make_graph_with_edge()
    try:
        created = rg.created()
        last_opened = rg.last_opened()
        last_updated = rg.last_updated()
        # All three are non-null wall-clock milliseconds — must be positive.
        assert created > 0
        assert last_opened > 0
        assert last_updated > 0
        # Sanity: last_updated must be at or after created.
        assert last_updated >= created
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_edge_time_terminals():
    """`earliest_edge_time` / `latest_edge_time` return event timestamps under
    the current view. Nullable — empty view returns None."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Only one edge, added at t=3.
        assert rg.earliest_edge_time() == 3
        assert rg.latest_edge_time() == 3

        # Add another edge at t=8. Range becomes [3, 8].
        rg.add_edge(8, "ben", "hamza")
        assert rg.earliest_edge_time() == 3
        assert rg.latest_edge_time() == 8

        # Windowed view narrows the range.
        assert rg.window(0, 5).earliest_edge_time() == 3
        assert rg.window(0, 5).latest_edge_time() == 3
        assert rg.window(6, 10).earliest_edge_time() == 8

        # Window with no edge events returns None.
        assert rg.window(100, 200).earliest_edge_time() is None
        assert rg.window(100, 200).latest_edge_time() is None
    finally:
        server_cm.__exit__(None, None, None)


def test_node_update_time_terminals():
    """`first_update` / `last_update` on a node return the range of event
    timestamps that touched this node under the current view."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # ben has add_node at t=1 and add_edge (ben, hamza) at t=3.
        ben = rg.node("ben")
        assert ben.first_update() == 1
        assert ben.last_update() == 3

        # Windowed view narrows the range — only the t=3 edge event visible.
        ben_windowed = rg.window(2, 5).node("ben")
        assert ben_windowed.first_update() == 3
        assert ben_windowed.last_update() == 3
    finally:
        server_cm.__exit__(None, None, None)


def test_terminals_error_on_absent_node_or_edge():
    """Any terminal on a node/edge that isn't visible under the current view
    raises with a semantic `not found in view` message — both nullable and
    non-nullable terminals treat the intermediate-null response as an error,
    since the server can't distinguish absent-from-graph vs. absent-from-view.
    """
    import pytest

    server_cm, rg = _make_graph_with_edge()
    try:
        # Absent from graph entirely.
        with pytest.raises(Exception, match="Node 'nonexistent' not found"):
            rg.node("nonexistent").degree()
        with pytest.raises(Exception, match="Node 'nonexistent' not found"):
            rg.node("nonexistent").earliest_time()
        with pytest.raises(Exception, match="Node 'nonexistent' not found"):
            rg.node("nonexistent").node_type()

        # Present in graph, but not visible in this window.
        with pytest.raises(Exception, match="Node 'ben' not found"):
            rg.window(100, 200).node("ben").degree()
        with pytest.raises(Exception, match="Node 'ben' not found"):
            rg.window(100, 200).node("ben").first_update()

        # Absent edges (via `.src().name()` — walks through the edge intermediate).
        with pytest.raises(Exception, match=r"Edge \('nonexistent', 'hamza'\) not found"):
            rg.edge("nonexistent", "hamza").src().name()
        with pytest.raises(Exception, match=r"Edge \('ben', 'hamza'\) not found"):
            rg.window(100, 200).edge("ben", "hamza").src().name()

        # Nullable terminal on an *existing* node with genuinely-missing data
        # still returns None (ben exists, no type ever set on him).
        assert rg.node("ben").node_type() is None
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_read_terminals():
    """Read terminals on RemoteEdge — time, layer, id, bool state — mirror
    the shape of the Node terminals under the current view."""
    server_cm, rg = _make_graph_with_edge()
    # Second edge event on the same pair at t=8, so we can distinguish
    # first_update vs last_update on the edge itself.
    rg.add_edge(8, "ben", "hamza")
    try:
        e = rg.edge("ben", "hamza")
        # Time-range terminals.
        assert e.earliest_time() == 3
        assert e.latest_time() == 8
        assert e.first_update() == 3
        assert e.last_update() == 8

        # Id — pair of endpoint ids.
        assert e.id() == ("ben", "hamza")

        # Layer info — layer_names lists all layers this edge appears in.
        assert e.layer_names() == ["_default"]
        # `.layer_name()` requires an `.explode()`'d view; on a plain edge
        # handle the server returns a GraphQL error which surfaces as
        # ClientError::GraphQLErrors. We surface the message unchanged — the
        # test just asserts we surface *something* with "layer_name" in it.
        import pytest
        with pytest.raises(Exception, match="layer_name"):
            e.layer_name()

        # Bool state.
        assert e.is_active() is True
        assert e.is_valid() is True
        assert e.is_deleted() is False
        assert e.is_self_loop() is False

        # Windowed view narrows time range.
        e_win = rg.window(0, 5).edge("ben", "hamza")
        assert e_win.earliest_time() == 3
        assert e_win.latest_time() == 3
        assert e_win.first_update() == 3
        assert e_win.last_update() == 3
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_self_loop_and_absent():
    """`is_self_loop` returns True for src == dst; absent edges error."""
    import pytest

    server_cm, rg = _make_graph_with_edge()
    # A self-loop edge.
    rg.add_edge(4, "ben", "ben")
    try:
        assert rg.edge("ben", "ben").is_self_loop() is True
        assert rg.edge("ben", "hamza").is_self_loop() is False

        # Absent edge → NotFound.
        with pytest.raises(Exception, match=r"Edge \('nonexistent', 'hamza'\) not found"):
            rg.edge("nonexistent", "hamza").is_active()
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_nbr_navigation():
    """`.nbr()` navigates to the "other end" node; on a plain edge it's
    equivalent to `.dst()`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        e = rg.edge("ben", "hamza")
        # On a plain (out-)edge view, nbr yields the destination.
        assert e.nbr().name() == "hamza"
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_view_chain_propagates_through_collection_list():
    """Regression: materialized edges must carry the parent view forward, so
    view-dependent terminals give the right answer under the same view chain
    that produced the collection."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Second edge event on the same (ben, hamza) pair at t=8.
        rg.add_edge(8, "ben", "hamza")

        # Global: one distinct edge, but edge_history_count on ben sees 2 events.
        # Window [0, 5): only t=3 event visible.
        # Window [6, 10): only t=8 event visible.

        # Iterate through the graph-level windowed edges collection.
        windowed_edges = list(rg.window(0, 5).edges)
        assert len(windowed_edges) == 1
        # The materialized edge should carry the window — its src() → node
        # should see edge_history_count == 1 under the window.
        for e in windowed_edges:
            assert e.src().edge_history_count() == 1, (
                "expected src().edge_history_count() == 1 under [0,5) window. "
                "If this is 2, the view chain isn't propagating through edges.list()."
            )

        # Also verify via node → out_edges navigation.
        windowed_out = list(rg.window(0, 5).node("ben").out_edges)
        assert len(windowed_out) == 1
        for e in windowed_out:
            assert e.src().edge_history_count() == 1
    finally:
        server_cm.__exit__(None, None, None)
