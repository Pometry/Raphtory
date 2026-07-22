"""End-to-end tests for the Transport-based RemoteGraph API.

Exercises the full stack: Python API → PyO3 wrapper → Rust RemoteGraph →
Op::Write/Op::Read → GraphqlTransport → GraphQL server → response back.

RPC model:
- View-op builders (`.window()`, `.layer()`, `.at()`, ...) are lazy — no RPC.
- Selection methods `.node()` / `.edge()` fire one validation RPC each (via
  hasNode / hasEdge) and return `None` if the id isn't present in the current
  view (matching the local `Graph.node -> Optional[Node]`).
- Terminals (`.degree()`, `.earliest_time()`, `.count()`, ...) fire one RPC
  evaluating the accumulated read expression.
- Writes (`.add_node()`, `.add_edge()`, ...) always fire an RPC.
"""

import tempfile

from raphtory.graphql import EdgeSortBy, GraphServer, NodeSortBy, SortByTime


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
        assert ben.name == "ben"
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
        assert rg.earliest_time == 1
        assert rg.latest_time == 3

        # Windowed [1, 3): earliest=1, latest=2 (hamza added at t=2; edge excluded).
        rg_win = rg.window(1, 3)
        assert rg_win.earliest_time == 1
        assert rg_win.latest_time == 2
        # Window bounds also come back through the same compound path.
        assert rg_win.start == 1
        assert rg_win.end == 3

        # On a Node — earliest/latest reflect the node's own events under the view.
        ben = rg.node("ben")
        assert ben.earliest_time == 1  # ben added at t=1
        assert ben.latest_time == 3    # participated in edge at t=3
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
        assert ben.id == "ben"
        assert ben.node_type is None  # type not set

        # Set a node type via the write path, then re-read.
        ben.set_node_type("person")
        assert rg.node("ben").node_type == "person"

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
    `.count()`, and `.collect()` terminals."""
    server_cm, rg = _make_graph_with_edge()
    try:
        nodes = rg.nodes
        assert nodes.count() == 2
        assert sorted(nodes.ids()) == ["ben", "hamza"]

        # Materialize as RemoteNode handles, then read a scalar off each.
        remote_nodes = nodes.collect()
        assert len(remote_nodes) == 2
        names = sorted(n.name for n in remote_nodes)
        assert names == ["ben", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_view_chain_propagates_through_collection_list():
    """Regression: previously `rg.window(...).nodes.collect()` rebased returned
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
            if n.name == "ben":
                windowed_counts.append(n.edge_history_count())
        assert windowed_counts == [1], (
            f"expected edge_history_count == 1 under [0,5) window, got {windowed_counts}. "
            "If this is 2, the view chain isn't propagating through .collect()."
        )

        # Also verify via out_neighbours navigation.
        for n in rg.window(0, 5).node("ben").out_neighbours:
            # hamza in [0, 5): only the t=3 edge — history count 1.
            assert n.edge_history_count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_native_iteration():
    """`for n in rg.nodes:` — no explicit `.collect()` needed."""
    server_cm, rg = _make_graph_with_edge()
    try:
        names = sorted(n.name for n in rg.nodes)
        assert names == ["ben", "hamza"]

        # Native iteration over a navigation collection.
        out_names = [n.name for n in rg.node("ben").out_neighbours]
        assert out_names == ["hamza"]

        # Iterating twice is idempotent (each iter() call fetches fresh).
        first = [n.name for n in rg.nodes]
        second = [n.name for n in rg.nodes]
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
        assert e.src.name == "ben"
        assert e.dst.name == "hamza"
        # The navigated-back node handles carry the full view chain — evaluating
        # a terminal on them fires an RPC against the same underlying edge.
        assert e.src.degree() == 1
        assert e.dst.degree() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_collection():
    """`rg.edges` accessor returns a `RemoteEdges` collection with `.count()`
    and `.collect()` terminals. Unlike nodes, edges have no `.ids()` — they're
    identified by `(src, dst)` pairs."""
    server_cm, rg = _make_graph_with_edge()
    try:
        edges = rg.edges
        assert edges.count() == 1

        # Materialize as RemoteEdge handles; navigate back to endpoints.
        remote_edges = edges.collect()
        assert len(remote_edges) == 1
        pairs = sorted((e.src.name, e.dst.name) for e in remote_edges)
        assert pairs == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_native_iteration():
    """`for e in rg.edges:` yields `RemoteEdge` handles without an explicit
    `.collect()` call."""
    server_cm, rg = _make_graph_with_edge()
    # Add a second edge so we can verify multi-edge iteration.
    rg.add_node(4, "sam")
    rg.add_edge(5, "ben", "sam")
    try:
        pairs = sorted((e.src.name, e.dst.name) for e in rg.edges)
        assert pairs == [("ben", "hamza"), ("ben", "sam")]

        # Native iteration over a node's out_edges collection.
        out_pairs = sorted(
            (e.src.name, e.dst.name) for e in rg.node("ben").out_edges
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
        out_pairs = [(e.src.name, e.dst.name) for e in ben.out_edges.collect()]
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


def test_absent_node_or_edge_returns_none():
    """`.node()` / `.edge()` return `None` when the id isn't present in the
    current view — matching the local `Graph.node -> Optional[Node]` — rather
    than raising. Covers both absent-from-graph and absent-from-window; the
    server can't distinguish the two, so both collapse to `None`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Absent from graph entirely → None.
        assert rg.node("nonexistent") is None

        # Present in graph, but not visible in this window → None.
        assert rg.window(100, 200).node("ben") is None

        # Absent edge (pair not present) → None.
        assert rg.edge("nonexistent", "hamza") is None

        # Edge present in graph but not visible in this window → None.
        assert rg.window(100, 200).edge("ben", "hamza") is None

        # Nullable terminal on an *existing* node with genuinely-missing data
        # still returns None (ben exists, no type ever set on him).
        assert rg.node("ben").node_type is None
    finally:
        server_cm.__exit__(None, None, None)


def test_node_view_chain_builders():
    """RemoteNode has full view-chain builder parity with the local Node —
    `.window`, `.at`, `.before`, `.after`, `.latest`, `.snapshot_at`,
    `.snapshot_latest`, `.shrink_*`, `.default_layer`, `.layer`, `.layers`,
    `.exclude_layer`, `.exclude_layers`. All lazy — no RPC until a terminal."""
    server_cm, rg = _make_graph_with_edge()
    # Add a second edge event on the same pair at t=8 so we can distinguish
    # windowed views clearly.
    rg.add_edge(8, "ben", "hamza")
    try:
        ben = rg.node("ben")

        # Global vs windowed on the same node handle.
        assert ben.edge_history_count() == 2      # two edge events total
        assert ben.window(0, 5).edge_history_count() == 1
        assert ben.window(6, 10).edge_history_count() == 1
        assert ben.window(100, 200).edge_history_count() == 0

        # At — snapshot at a specific time.
        assert ben.at(3).is_active() is True
        assert ben.at(5).is_active() is False     # window [5, 6) — no events

        # Before / after — one-sided views.
        assert ben.before(5).edge_history_count() == 1   # only t=3
        assert ben.after(5).edge_history_count() == 1    # only t=8
        assert ben.before(0).edge_history_count() == 0
        assert ben.after(100).edge_history_count() == 0

        # Latest / snapshot_latest — non-argumentative view ops compile through.
        assert ben.latest().degree() == 1
        assert ben.snapshot_latest().degree() == 1
        assert ben.snapshot_at(3).degree() == 1

        # Layer ops — default_layer, layer, layers, exclude_layer, exclude_layers.
        assert ben.default_layer().degree() == 1
        assert ben.layer("_default").degree() == 1
        assert ben.layers(["_default"]).degree() == 1
        assert ben.exclude_layer("_default").degree() == 0
        assert ben.exclude_layers(["_default"]).degree() == 0

        # Chaining works — window then out_neighbours.
        neighbours = ben.window(0, 5).out_neighbours.ids()
        assert neighbours == ["hamza"]
        assert ben.window(100, 200).out_neighbours.count() == 0

        # Chain after selection order commutes with pre-selection.
        assert ben.window(0, 5).degree() == rg.window(0, 5).node("ben").degree()
    finally:
        server_cm.__exit__(None, None, None)


def test_node_shrink_builders():
    """`.shrink_window`, `.shrink_start`, `.shrink_end` narrow an existing window."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_edge(8, "ben", "hamza")
    try:
        # Start from a wide window, then shrink it.
        wide = rg.node("ben").window(0, 100)
        assert wide.edge_history_count() == 2

        # Shrink both ends.
        assert wide.shrink_window(0, 5).edge_history_count() == 1
        # Shrink start only — cuts off t=3, keeps t=8.
        assert wide.shrink_start(5).edge_history_count() == 1
        # Shrink end only — keeps t=3, cuts off t=8.
        assert wide.shrink_end(5).edge_history_count() == 1
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
        assert e.earliest_time == 3
        assert e.latest_time == 8
        assert e.first_update() == 3
        assert e.last_update() == 8

        # Id — pair of endpoint ids.
        assert e.id == ("ben", "hamza")

        # Layer info — layer_names lists all layers this edge appears in.
        assert e.layer_names == ["_default"]
        # `.layer_name()` requires an `.explode()`'d view; on a plain edge
        # handle the server returns a GraphQL error which surfaces as
        # ClientError::GraphQLErrors. We surface the message unchanged — the
        # test just asserts we surface *something* with "layer_name" in it.
        import pytest
        with pytest.raises(Exception, match="layer_name"):
            e.layer_name

        # Bool state.
        assert e.is_active() is True
        assert e.is_valid() is True
        assert e.is_deleted() is False
        assert e.is_self_loop() is False

        # Windowed view narrows time range.
        e_win = rg.window(0, 5).edge("ben", "hamza")
        assert e_win.earliest_time == 3
        assert e_win.latest_time == 3
        assert e_win.first_update() == 3
        assert e_win.last_update() == 3
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_self_loop_and_absent():
    """`is_self_loop` returns True for src == dst; absent edges return None."""
    server_cm, rg = _make_graph_with_edge()
    # A self-loop edge.
    rg.add_edge(4, "ben", "ben")
    try:
        assert rg.edge("ben", "ben").is_self_loop() is True
        assert rg.edge("ben", "hamza").is_self_loop() is False

        # Absent edge → None (not an error).
        assert rg.edge("nonexistent", "hamza") is None
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_nbr_navigation():
    """`.nbr()` navigates to the "other end" node; on a plain edge it's
    equivalent to `.dst()`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        e = rg.edge("ben", "hamza")
        # On a plain (out-)edge view, nbr yields the destination.
        assert e.nbr.name == "hamza"
    finally:
        server_cm.__exit__(None, None, None)


def test_collection_view_chain_builders():
    """RemoteNodes and RemoteEdges have full view-chain builder parity with
    the parent Graph — `.window`, `.at`, `.before`, `.after`, `.latest`,
    `.snapshot_at`, `.snapshot_latest`, `.shrink_*`, `.default_layer`,
    `.layer`, `.layers`, `.exclude_layer`, `.exclude_layers`. All lazy."""
    server_cm, rg = _make_graph_with_edge()
    # Add a second edge event to distinguish windowed views clearly.
    rg.add_edge(8, "ben", "hamza")
    try:
        # Collection membership is "sticky" — narrowing the view of an already-
        # materialized `.nodes` / `.edges` handle doesn't change its count.
        # Contrast with pre-selection (`rg.window(...).nodes`) where the graph-
        # level view filters membership. Same semantics as node/edge selection.
        assert rg.nodes.window(0, 5).count() == 2
        assert rg.nodes.window(100, 200).count() == 2   # sticky!
        assert rg.window(100, 200).nodes.count() == 0   # graph-level filters
        # Same story on edges — collection membership sticks; view narrows.
        assert rg.edges.window(0, 5).count() == 1
        assert rg.edges.window(100, 200).count() == 1   # sticky
        assert rg.window(100, 200).edges.count() == 0   # graph-level filters

        # at / before / after / latest / snapshot compose without membership change on nodes.
        assert rg.nodes.at(3).count() == 2
        assert rg.nodes.before(5).count() == 2
        assert rg.nodes.after(5).count() == 2
        assert rg.nodes.latest().count() == 2
        assert rg.nodes.snapshot_latest().count() == 2
        assert rg.nodes.snapshot_at(3).count() == 2

        # Layer ops on edges — same sticky semantics: count unchanged, view narrows.
        assert rg.edges.default_layer().count() == 1
        assert rg.edges.layer("_default").count() == 1
        assert rg.edges.layers(["_default"]).count() == 1
        assert rg.edges.exclude_layer("_default").count() == 1
        assert rg.edges.exclude_layers(["_default"]).count() == 1

        # `.start` reflects the collection's own view bound.
        assert rg.nodes.window(0, 5).start == 0
        assert rg.nodes.window(0, 5).end == 5
    finally:
        server_cm.__exit__(None, None, None)


def test_collection_view_chain_composes_with_materialization():
    """Materialized handles from a view-narrowed collection carry the view
    forward — tests `base_graph` propagation through view builders on the
    collection. `for n in ...:` uses `__iter__` which delegates to `.collect()`;
    both paths hit the same base_graph plumbing."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_edge(8, "ben", "hamza")
    try:
        # Iterate over a window-narrowed collection — each yielded handle
        # should see the windowed view.
        for n in rg.nodes.window(0, 5):
            if n.name == "ben":
                # Only the t=3 edge is visible in [0, 5) — ben's history count is 1.
                assert n.edge_history_count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_node_view_chain_propagates_through_neighbour_materialization():
    """Regression for the same `base_graph` bug — but on `RemoteNode`. If
    view builders on Node don't update `base_graph`, then materialized
    neighbours would revert to the unwindowed graph view."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_edge(8, "ben", "hamza")
    try:
        # Take ben, narrow to [0, 5), then materialize his out_neighbours.
        # Each neighbour should still see the windowed view — meaning
        # hamza's edge_history_count under that view is 1, not 2.
        for n in rg.node("ben").window(0, 5).out_neighbours:
            assert n.name == "hamza"
            assert n.edge_history_count() == 1, (
                "expected 1 under [0,5) window. If this is 2, base_graph is "
                "not propagating through RemoteNode's view builders."
            )
    finally:
        server_cm.__exit__(None, None, None)


def test_history_scalar_terminals_on_node():
    """`node.history` returns a `RemoteHistory` container with scalar
    terminals — `count`, `is_empty`, `earliest_time`, `latest_time`. Access
    is via property (matching local API), not method."""
    server_cm, rg = _make_graph_with_edge()
    # Node ben: add_node at t=1, add_edge (ben, hamza) at t=3 → 2 events.
    try:
        h = rg.node("ben").history        # property, not method
        assert h.count() == 2
        assert h.is_empty() is False
        assert h.earliest_time() == 1
        assert h.latest_time() == 3

        # Under a window that excludes both events, count is 0 (view narrows,
        # but the node selection itself is validated at .node() and passes
        # because ben exists in the outer view).
        h_windowed = rg.node("ben").window(100, 200).history
        assert h_windowed.count() == 0
        assert h_windowed.is_empty() is True
        assert h_windowed.earliest_time() is None
        assert h_windowed.latest_time() is None
    finally:
        server_cm.__exit__(None, None, None)


def test_history_scalar_terminals_on_edge():
    """`edge.history` and `edge.deletions` — both return `RemoteHistory`
    handles but read different server fields."""
    server_cm, rg = _make_graph_with_edge()
    # Edge (ben, hamza): one event at t=3, no deletions.
    try:
        e = rg.edge("ben", "hamza")

        h = e.history
        assert h.count() == 1
        assert h.is_empty() is False
        assert h.earliest_time() == 3
        assert h.latest_time() == 3

        d = e.deletions
        assert d.count() == 0
        assert d.is_empty() is True
        assert d.earliest_time() is None
        assert d.latest_time() is None
    finally:
        server_cm.__exit__(None, None, None)


def test_history_list_and_iter():
    """`history.collect()` returns `List[RemoteEventTime]` sorted ascending by
    time; `.collect_rev()` returns them descending. `for t in history:` iterates
    via `__iter__` which delegates to `.collect()`."""
    server_cm, rg = _make_graph_with_edge()
    # ben has events at t=1 (add_node) and t=3 (add_edge). Add another at t=8.
    rg.add_edge(8, "ben", "hamza")
    try:
        h = rg.node("ben").history
        events = h.collect()
        assert len(events) == 3
        # Extract timestamps — dt/event_id are also populated but shape-check
        # them separately below.
        assert [e.timestamp for e in events] == [1, 3, 8]

        # list_rev
        events_rev = h.collect_rev()
        assert [e.timestamp for e in events_rev] == [8, 3, 1]

        # Iterator delegates to .collect() — same order.
        via_iter = [e.timestamp for e in h]
        assert via_iter == [1, 3, 8]

        # All three fields populated by the server. dt is RFC 3339.
        for e in events:
            assert e.timestamp is not None
            assert e.event_id is not None
            assert e.dt is not None and "T" in e.dt   # ISO 8601 has 'T' separator
    finally:
        server_cm.__exit__(None, None, None)


def test_history_list_on_empty_view():
    """`.collect()` on an empty history returns an empty list, not None."""
    server_cm, rg = _make_graph_with_edge()
    try:
        empty = rg.node("ben").window(100, 200).history
        assert empty.collect() == []
        assert empty.collect_rev() == []
        assert list(empty) == []                       # iteration also empty
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_schema():
    """`rg.schema()` fires one RPC and returns the full schema tree —
    node types + edge layers + their property/metadata schemas."""
    server_cm, rg = _make_graph_with_edge()
    # Node types + temporal properties + metadata to make the schema
    # interesting.
    rg.node("ben").set_node_type("user")
    rg.node("hamza").set_node_type("bot")
    rg.node("ben").add_updates(5, properties={"score": 1.5})
    rg.node("ben").add_metadata({"role": "admin"})
    rg.edge("ben", "hamza").add_metadata({"weight": 0.5})
    try:
        schema = rg.schema()

        # nodes: one entry per node type
        node_types = sorted(n.type_name for n in schema.nodes)
        assert "user" in node_types
        assert "bot" in node_types

        # user node type has a "score" temporal property
        user_schema = next(n for n in schema.nodes if n.type_name == "user")
        score_prop = next(
            (p for p in user_schema.properties if p.key == "score"), None
        )
        assert score_prop is not None
        assert score_prop.property_type  # some type string

        # user node type has "role" metadata
        role_meta = next((p for p in user_schema.metadata if p.key == "role"), None)
        assert role_meta is not None

        # layers: default layer with edges
        default_layer = next((l for l in schema.layers if l.name == "_default"), None)
        assert default_layer is not None
        assert len(default_layer.edges) >= 1

        # edge schema: user → bot with weight metadata
        edge_schema = default_layer.edges[0]
        assert edge_schema.src_type in {"user", "bot"}
        assert edge_schema.dst_type in {"user", "bot"}
        weight_meta = next(
            (p for p in edge_schema.metadata if p.key == "weight"), None
        )
        assert weight_meta is not None
    finally:
        server_cm.__exit__(None, None, None)


def test_temporal_property_stats():
    """`RemoteTemporalProperty` numeric stats: sum, mean, average, min, max,
    median. Non-numeric aggregates return None. Non-numeric stats return
    `RemotePropertyTuple` with a time and native-Python value."""
    server_cm, rg = _make_graph_with_edge()
    # Numeric values: 1, 2, 3, 4, 5
    for i, t in enumerate([1, 2, 3, 4, 5]):
        rg.node("ben").add_updates(t, properties={"score": float(i + 1)})
    try:
        score = rg.node("ben").properties.temporal.get("score")

        # Numeric aggregates on floats: sum=15, mean=3.0, average=3.0
        assert score.sum() == 15.0
        assert score.mean() == 3.0
        assert score.average() == 3.0

        # Min/max/median return RemotePropertyTuple (time + value)
        mn = score.min()
        assert mn is not None
        assert mn.value == 1.0
        assert mn.time.timestamp == 1

        mx = score.max()
        assert mx is not None
        assert mx.value == 5.0
        assert mx.time.timestamp == 5

        med = score.median()
        assert med is not None
        assert med.value == 3.0
        assert med.time.timestamp == 3
    finally:
        server_cm.__exit__(None, None, None)


def test_temporal_property_unique_and_dedupe():
    """`.unique()` returns distinct values; `.ordered_dedupe(latest_time)`
    collapses runs of consecutive-equal values."""
    server_cm, rg = _make_graph_with_edge()
    # Runs of equal values: 1, 1, 2, 2, 2, 3, 1
    for t, v in [(1, 1), (2, 1), (3, 2), (4, 2), (5, 2), (6, 3), (7, 1)]:
        rg.node("ben").add_updates(t, properties={"status": v})
    try:
        status = rg.node("ben").properties.temporal.get("status")

        # Distinct values — order not guaranteed
        assert sorted(status.unique()) == [1, 2, 3]

        # ordered_dedupe(latest_time=False): (1, 1), (3, 2), (6, 3), (7, 1) — first
        # timestamp of each run.
        first_ts = status.ordered_dedupe(latest_time=False)
        assert [(p.time.timestamp, p.value) for p in first_ts] == [
            (1, 1), (3, 2), (6, 3), (7, 1)
        ]

        # ordered_dedupe(latest_time=True): (2, 1), (5, 2), (6, 3), (7, 1) — last
        # timestamp of each run.
        last_ts = status.ordered_dedupe(latest_time=True)
        assert [(p.time.timestamp, p.value) for p in last_ts] == [
            (2, 1), (5, 2), (6, 3), (7, 1)
        ]
    finally:
        server_cm.__exit__(None, None, None)


def test_temporal_properties_container():
    """`properties.temporal` returns a `RemoteTemporalProperties` container.
    `.get(key)` returns a `RemoteTemporalProperty` handle if present, `None`
    otherwise. `.values()` returns handles for every temporal property."""
    server_cm, rg = _make_graph_with_edge()
    rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
    rg.node("ben").add_updates(10, properties={"score": 2.5})
    try:
        tp = rg.node("ben").properties.temporal

        # keys
        assert sorted(tp.keys()) == ["active", "score"]

        # contains
        assert tp.contains("score") is True
        assert tp.contains("nonexistent") is False

        # get — Optional[RemoteTemporalProperty]
        score = tp.get("score")
        assert score is not None
        assert score.key == "score"

        assert tp.get("nonexistent") is None

        # values — list of handles
        handles = tp.values()
        by_key = {h.key: h for h in handles}
        assert set(by_key.keys()) == {"score", "active"}

        # values with whitelist
        subset = tp.values(keys=["score"])
        assert [h.key for h in subset] == ["score"]
    finally:
        server_cm.__exit__(None, None, None)


def test_temporal_property_terminals():
    """`RemoteTemporalProperty` core methods: `.history`, `.values()`,
    `.at(t)`, `.latest()`, `.count()`."""
    server_cm, rg = _make_graph_with_edge()
    # score: 1.5 at t=5, 2.5 at t=10, 3.5 at t=15
    rg.node("ben").add_updates(5, properties={"score": 1.5})
    rg.node("ben").add_updates(10, properties={"score": 2.5})
    rg.node("ben").add_updates(15, properties={"score": 3.5})
    try:
        score = rg.node("ben").properties.temporal.get("score")

        # count — number of updates
        assert score.count() == 3

        # values — all values in temporal order
        vals = score.values()
        assert vals == [1.5, 2.5, 3.5]

        # latest — most recent value
        assert score.latest() == 3.5

        # at(t) — value at or before t
        assert score.at(5) == 1.5
        assert score.at(7) == 1.5   # no update at 7 → latest before is at t=5
        assert score.at(10) == 2.5
        assert score.at(100) == 3.5  # latest before 100 is 3.5

        # at(t) before any update — None
        assert score.at(0) is None

        # history — reuses RemoteHistory
        hist = score.history
        assert hist.count() == 3
        assert hist.collect()[0].timestamp == 5
    finally:
        server_cm.__exit__(None, None, None)


def test_node_properties_basic():
    """`node.properties` returns a `RemoteProperties` container (temporal +
    metadata). Same terminal shape as metadata; for temporal properties,
    `.get(key)` and `.values()` return the property's most recent value."""
    server_cm, rg = _make_graph_with_edge()
    # Add temporal properties at t=5, t=10.
    rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
    rg.node("ben").add_updates(10, properties={"score": 2.5})
    try:
        props = rg.node("ben").properties

        # keys — all temporal property names.
        assert sorted(props.keys()) == ["active", "score"]

        # contains — bool.
        assert props.contains("score") is True
        assert props.contains("nonexistent") is False

        # get — Optional[RemoteProperty]. For a temporal property, returns
        # the latest value under the current view (t=10 → score=2.5).
        score = props.get("score")
        assert score is not None
        assert score == 2.5

        # get on missing key — None.
        assert props.get("nonexistent") is None

        # items — (key, value) pairs.
        by_key = dict(props.items())
        assert by_key == {"score": 2.5, "active": True}

        # values with whitelist — raw values for just the named keys.
        subset = props.values(keys=["score"])
        assert subset == [2.5]
    finally:
        server_cm.__exit__(None, None, None)


def test_properties_vs_metadata_separation():
    """`.properties` covers temporal properties; `.metadata` covers non-
    temporal. Server exposes them as separate containers — no overlap in
    keys."""
    server_cm, rg = _make_graph_with_edge()
    rg.node("ben").add_metadata({"role": "admin"})            # non-temporal
    rg.node("ben").add_updates(5, properties={"score": 1.0})   # temporal
    try:
        # Metadata has "role", properties has "score" — no cross-contamination.
        assert rg.node("ben").metadata.keys() == ["role"]
        assert rg.node("ben").properties.keys() == ["score"]

        # get() on the wrong container returns None.
        assert rg.node("ben").metadata.get("score") is None
        assert rg.node("ben").properties.get("role") is None
    finally:
        server_cm.__exit__(None, None, None)


def test_node_metadata_basic():
    """`node.metadata` returns a `RemoteMetadata` container. Standard shape:
    `get(key)`, `contains(key)`, `keys()`, `values(keys=None)`. Values are
    native Python types via raphtory's Prop → Python conversion."""
    server_cm, rg = _make_graph_with_edge()
    # Attach metadata to ben (non-temporal).
    rg.node("ben").add_metadata({"role": "admin", "level": 3, "active": True})
    try:
        md = rg.node("ben").metadata

        # keys — all names present.
        assert sorted(md.keys()) == ["active", "level", "role"]

        # contains — bool per key.
        assert md.contains("role") is True
        assert md.contains("nonexistent") is False

        # get — Optional[RemoteProperty], value is native Python type.
        role = md.get("role")
        assert role is not None
        assert role == "admin"

        level = md.get("level")
        assert level == 3            # int
        active = md.get("active")
        assert active is True        # bool

        # get on missing key — None.
        assert md.get("nonexistent") is None

        # values — list of RemoteProperty, all entries.
        all_values = md.values()
        assert len(all_values) == 3
        by_key = dict(md.items())
        assert by_key == {"role": "admin", "level": 3, "active": True}

        # values with whitelist — raw values for just the named keys.
        subset = md.values(keys=["role", "level"])
        assert sorted(subset, key=str) == [3, "admin"]
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_and_edge_metadata():
    """`.metadata` accessor exists on RemoteGraph, RemoteNode, and RemoteEdge
    — same container shape."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_metadata({"description": "test graph"})
    rg.edge("ben", "hamza").add_metadata({"weight": 5.5})
    try:
        # Graph metadata
        assert rg.metadata.get("description") == "test graph"

        # Edge metadata
        weight = rg.edge("ben", "hamza").metadata.get("weight")
        assert weight is not None
        assert weight == 5.5
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_explode():
    """`.explode()` on a `RemoteEdge` fans it out into one entry per event,
    returning a `RemoteEdges` collection. `explode_layers()` fans out by layer."""
    server_cm, rg = _make_graph_with_edge()
    # Add multiple events on the same edge.
    rg.add_edge(5, "ben", "hamza")
    rg.add_edge(8, "ben", "hamza")
    try:
        e = rg.edge("ben", "hamza")
        # 3 events on this edge: t=3, t=5, t=8.
        exploded = e.explode()
        assert exploded.count() == 3

        # Each exploded instance still points at (ben, hamza).
        for ex in exploded.collect():
            assert ex.src.name == "ben"
            assert ex.dst.name == "hamza"

        # Layer explode — only one layer here so should be 1 entry.
        by_layer = e.explode_layers()
        assert by_layer.count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_explode():
    """`.explode()` on a `RemoteEdges` collection expands each member into
    its events. Terminal count reflects the sum of per-edge event counts."""
    server_cm, rg = _make_graph_with_edge()
    # Two edges, ben->hamza with events at t=3 and t=5, ben->sam with event at t=7.
    rg.add_edge(5, "ben", "hamza")
    rg.add_node(6, "sam")
    rg.add_edge(7, "ben", "sam")
    try:
        # Total events across both edges: 2 + 1 = 3.
        exploded = rg.edges.explode()
        assert exploded.count() == 3
    finally:
        server_cm.__exit__(None, None, None)


def test_node_in_out_component():
    """`.in_component` / `.out_component` return the set of ancestors /
    descendants reachable via directed edges (excluding self). Both are
    `RemoteNodes` handles with the usual terminals (count, ids, list, iter)."""
    server_cm, rg = _make_graph_with_edge()
    # Build a chain: ben -> hamza -> sam -> tom  (t=3 already has ben->hamza)
    rg.add_node(4, "sam")
    rg.add_node(5, "tom")
    rg.add_edge(4, "hamza", "sam")
    rg.add_edge(5, "sam", "tom")
    try:
        # Out-component from ben: {hamza, sam, tom} (descendants, excludes ben).
        out = rg.node("ben").out_component
        assert sorted(out.ids()) == ["hamza", "sam", "tom"]
        assert out.count() == 3

        # In-component of tom: {ben, hamza, sam}.
        into_tom = rg.node("tom").in_component
        assert sorted(into_tom.ids()) == ["ben", "hamza", "sam"]

        # Sam sits in the middle — in-component {ben, hamza}, out-component {tom}.
        assert sorted(rg.node("sam").in_component.ids()) == ["ben", "hamza"]
        assert rg.node("sam").out_component.ids() == ["tom"]

        # Terminal node in out-direction: tom's out-component is empty.
        assert rg.node("tom").out_component.ids() == []
        assert rg.node("tom").out_component.count() == 0

        # Composes with view — under a window that only sees ben->hamza,
        # ben's out-component shrinks to {hamza}.
        windowed = rg.window(0, 4).node("ben").out_component
        assert sorted(windowed.ids()) == ["hamza"]

        # Iteration works.
        names = sorted(n.name for n in rg.node("ben").out_component)
        assert names == ["hamza", "sam", "tom"]
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_type_filter():
    """`rg.nodes.type_filter(types)` filters membership — the returned
    collection has fewer members. Distinct from view ops (window/layer/etc.)
    which are sticky and preserve membership."""
    server_cm, rg = _make_graph_with_edge()
    # Give the nodes distinct types.
    rg.node("ben").set_node_type("user")
    rg.node("hamza").set_node_type("bot")
    # Add a third node with no type.
    rg.add_node(4, "sam")
    try:
        all_nodes = rg.nodes
        assert all_nodes.count() == 3

        # Filter to only "user" nodes.
        users = all_nodes.type_filter(["user"])
        assert users.count() == 1
        assert users.ids() == ["ben"]

        # Filter to multiple types.
        both = all_nodes.type_filter(["user", "bot"])
        assert both.count() == 2
        assert sorted(both.ids()) == ["ben", "hamza"]

        # Filter to nonexistent type — empty collection.
        empty = all_nodes.type_filter(["nonexistent"])
        assert empty.count() == 0
        assert empty.ids() == []

        # Filter is composable — narrow further by a window.
        assert all_nodes.type_filter(["user"]).window(0, 5).count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_type_filter_with_windowed_view():
    """`type_filter` composes with view ops in any order — window then filter,
    filter then window, or graph-level window then nodes then filter."""
    server_cm, rg = _make_graph_with_edge()
    # ben (t=1) and hamza (t=2) are "user"; sam (t=10) is "user" but only
    # appears in the view after t=10.
    rg.add_node(10, "sam")
    rg.node("ben").set_node_type("user")
    rg.node("hamza").set_node_type("bot")
    rg.node("sam").set_node_type("user")
    try:
        # (a) Graph-scope window pre-selection → nodes filters membership by
        # window; then type_filter filters by type. Only ben matches "user"
        # in [0, 5) window.
        pre_windowed = rg.window(0, 5).nodes.type_filter(["user"])
        assert pre_windowed.count() == 1
        assert pre_windowed.ids() == ["ben"]

        # Materialize under the windowed filter — `.collect()` returns handles
        # rebased under the windowed graph. This is the regression path that
        # the base_graph propagation on view builders + Nodes-only filter
        # design has to handle correctly.
        materialized = pre_windowed.collect()
        assert len(materialized) == 1
        assert materialized[0].name == "ben"

        # (b) Sticky-selection: nodes fixed at 3, then windowed view narrows
        # (sticky, count unchanged), then type_filter shrinks to matching type.
        assert rg.nodes.window(0, 5).type_filter(["user"]).count() == 2

        # (c) Filter first, then window (still sticky — filter shrunk to 2,
        # window narrows view of those 2, count unchanged at 2).
        assert rg.nodes.type_filter(["user"]).window(0, 5).count() == 2
    finally:
        server_cm.__exit__(None, None, None)


def test_history_sub_containers():
    """`history.timestamps`, `.datetimes`, `.event_id`, `.intervals` — four
    parallel projections of the same events. Timestamps/event_id/intervals
    return `list[int]`; datetimes return `list[str]` (RFC 3339)."""
    server_cm, rg = _make_graph_with_edge()
    # ben events: add_node t=1, add_edge t=3. Add more so intervals are non-trivial.
    rg.add_edge(5, "ben", "hamza")
    rg.add_edge(9, "ben", "hamza")
    try:
        h = rg.node("ben").history

        # Timestamps view — plain ints
        assert h.timestamps.collect() == [1, 3, 5, 9]
        assert h.timestamps.collect_rev() == [9, 5, 3, 1]

        # DateTimes view — ISO strings, positionally aligned with timestamps
        dts = h.datetimes.collect()
        assert len(dts) == 4
        for s in dts:
            assert "T" in s   # RFC 3339 separator

        # Event IDs view — plain ints; server picks per-timestamp
        eids = h.event_id.collect()
        assert len(eids) == 4

        # Intervals view — deltas between consecutive events: 3-1=2, 5-3=2, 9-5=4
        intervals = h.intervals.collect()
        assert intervals == [2, 2, 4]
    finally:
        server_cm.__exit__(None, None, None)


def test_intervals_stats():
    """`intervals.mean()`, `.median()`, `.max()`, `.min()` — summary stats
    over inter-event gaps."""
    server_cm, rg = _make_graph_with_edge()
    # ben events: t=1, t=3. Add more to make intervals meaningful: [2, 2, 4].
    rg.add_edge(5, "ben", "hamza")
    rg.add_edge(9, "ben", "hamza")
    try:
        stats = rg.node("ben").history.intervals

        # intervals = [2, 2, 4], mean = 8/3 ≈ 2.666...
        mean = stats.mean()
        assert mean is not None
        assert abs(mean - 8.0 / 3.0) < 1e-9

        assert stats.median() == 2
        assert stats.max() == 4
        assert stats.min() == 2
    finally:
        server_cm.__exit__(None, None, None)


def test_sub_container_paging():
    """Sub-containers share the same `page(limit, offset, page_index)` shape
    as the root `RemoteHistory`."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_edge(5, "ben", "hamza")
    rg.add_edge(7, "ben", "hamza")
    rg.add_edge(9, "ben", "hamza")
    try:
        ts = rg.node("ben").history.timestamps
        # Full events: [1, 3, 5, 7, 9]
        assert ts.collect() == [1, 3, 5, 7, 9]
        assert ts.page(limit=2) == [1, 3]
        assert ts.page(limit=2, offset=2) == [5, 7]
        assert ts.page(limit=2, page_index=1) == [5, 7]   # equivalent
        assert ts.page_rev(limit=2) == [9, 7]
    finally:
        server_cm.__exit__(None, None, None)


def test_history_page_and_page_rev():
    """`history.page(limit, offset, page_index)` returns a slice of events;
    `.page_rev(...)` returns the equivalent slice in descending order.
    `offset` and `page_index` default to 0."""
    server_cm, rg = _make_graph_with_edge()
    # Add extra edges so ben has 5 events total: add_node at t=1, edges at
    # t=3, t=5, t=7, t=9.
    rg.add_edge(5, "ben", "hamza")
    rg.add_edge(7, "ben", "hamza")
    rg.add_edge(9, "ben", "hamza")
    try:
        h = rg.node("ben").history
        assert h.count() == 5

        # Full first page — limit=2, no offset, no page_index.
        page = h.page(limit=2)
        assert [e.timestamp for e in page] == [1, 3]

        # Explicit offset — skip 2, take 2.
        page_off = h.page(limit=2, offset=2)
        assert [e.timestamp for e in page_off] == [5, 7]

        # page_index=1 with limit=2 → skip 2, take 2 (equivalent to offset=2).
        page_idx = h.page(limit=2, page_index=1)
        assert [e.timestamp for e in page_idx] == [5, 7]

        # page_index=1 with limit=2 AND offset=1 → skip 2+1=3, take 2.
        page_combo = h.page(limit=2, offset=1, page_index=1)
        assert [e.timestamp for e in page_combo] == [7, 9]

        # Limit exceeds remaining — returns whatever is left.
        page_last = h.page(limit=10, offset=3)
        assert [e.timestamp for e in page_last] == [7, 9]

        # Reverse — first page in descending order.
        page_rev = h.page_rev(limit=2)
        assert [e.timestamp for e in page_rev] == [9, 7]

        # Reverse with offset.
        page_rev_off = h.page_rev(limit=2, offset=1)
        assert [e.timestamp for e in page_rev_off] == [7, 5]
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_history_and_deletions_lists():
    """Edge history and deletions both expose `.collect()` returning
    `RemoteEventTime`s under the same shape."""
    server_cm, rg = _make_graph_with_edge()
    # Add a deletion event at t=10.
    rg.delete_edge(10, "ben", "hamza")
    try:
        e = rg.edge("ben", "hamza")

        # Deletions has exactly one entry at t=10.
        deletion_events = e.deletions.collect()
        assert len(deletion_events) == 1
        assert deletion_events[0].timestamp == 10

        # History exposes non-deletion events.
        history_events = e.history.collect()
        assert len(history_events) >= 1
        assert all(ev.timestamp is not None for ev in history_events)
    finally:
        server_cm.__exit__(None, None, None)


def test_history_records_deletion_event():
    """After `.delete_edge()`, the edge's `.deletions` history includes the
    deletion time; `.history` reflects the add event."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Delete the ben→hamza edge at t=10.
        rg.delete_edge(10, "ben", "hamza")

        e = rg.edge("ben", "hamza")
        assert e.deletions.count() == 1
        assert e.deletions.earliest_time() == 10
    finally:
        server_cm.__exit__(None, None, None)


def test_collection_view_bounds():
    """`.start()` / `.end()` on RemoteNodes and RemoteEdges report the
    inherited view bound. `None` when the parent view is unbounded, matching
    the semantics on Graph / Node / Edge."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Unbounded — both bounds are None.
        assert rg.nodes.start is None
        assert rg.nodes.end is None
        assert rg.edges.start is None
        assert rg.edges.end is None

        # Bounded via graph-level window — inherited by collections.
        assert rg.window(0, 5).nodes.start == 0
        assert rg.window(0, 5).nodes.end == 5
        assert rg.window(0, 5).edges.start == 0
        assert rg.window(0, 5).edges.end == 5

        # One-sided bounds propagate to collections too.
        # `before(5)` is exclusive upper — end reports the boundary time.
        assert rg.before(5).nodes.start is None
        assert rg.before(5).nodes.end == 5
        # `after(5)` is exclusive lower — effective start is 6.
        assert rg.after(5).edges.start == 6
        assert rg.after(5).edges.end is None
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_unique_layers():
    """`unique_layers` returns the list of layer names present in the graph."""
    server_cm, rg = _make_graph_with_edge()
    try:
        assert rg.unique_layers == ["_default"]

        # Add an edge on a distinct layer.
        rg.add_edge(4, "ben", "hamza", layer="secret")
        # Now two layers are present.
        assert sorted(rg.unique_layers) == ["_default", "secret"]
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_view_chain_builders():
    """RemoteEdge has full view-chain builder parity with the local Edge —
    `.window`, `.at`, `.before`, `.after`, `.latest`, `.snapshot_at`,
    `.snapshot_latest`, `.shrink_*`, `.default_layer`, `.layer`, `.layers`,
    `.exclude_layer`, `.exclude_layers`. All lazy — no RPC until a terminal."""
    server_cm, rg = _make_graph_with_edge()
    # Add a second edge event on the same pair at t=8.
    rg.add_edge(8, "ben", "hamza")
    try:
        e = rg.edge("ben", "hamza")

        # Global time range: [3, 8].
        assert e.earliest_time == 3
        assert e.latest_time == 8

        # Windowed narrows the range.
        assert e.window(0, 5).earliest_time == 3
        assert e.window(0, 5).latest_time == 3
        assert e.window(6, 10).earliest_time == 8
        # Empty window — edge selection is preserved (we selected first, then
        # windowed), so nullable terminals return None. This differs from
        # `rg.window(...).edge(...)`, where the edge isn't present in the
        # windowed view at selection time so `.edge()` itself returns None.
        assert e.window(100, 200).earliest_time is None
        assert e.window(100, 200).latest_time is None
        import pytest

        # At / snapshot_at.
        assert e.at(3).is_active() is True
        assert e.snapshot_at(3).is_active() is True

        # Before / after — one-sided views.
        assert e.before(5).earliest_time == 3
        assert e.before(5).latest_time == 3
        assert e.after(5).earliest_time == 8
        assert e.after(5).latest_time == 8

        # Latest / snapshot_latest — non-argumentative view ops.
        assert e.latest().is_active() is True
        assert e.snapshot_latest().is_active() is True

        # Layer ops.
        assert e.default_layer().is_active() is True
        assert e.layer("_default").is_active() is True
        assert e.layers(["_default"]).is_active() is True
        # Exclude the only layer → edge selection preserved, view has no
        # visible events, so `is_active` reports False (not NotFound).
        assert e.exclude_layer("_default").is_active() is False
        assert e.exclude_layers(["_default"]).is_active() is False

        # Chained view composes with navigation.
        assert e.window(0, 5).src.name == "ben"
        assert e.window(0, 5).nbr.name == "hamza"

        # Commutativity: pre-selection view chain matches post-selection view chain.
        assert e.window(0, 5).earliest_time == rg.window(0, 5).edge("ben", "hamza").earliest_time
    finally:
        server_cm.__exit__(None, None, None)


def test_edge_shrink_builders():
    """`.shrink_window`, `.shrink_start`, `.shrink_end` narrow an existing window."""
    server_cm, rg = _make_graph_with_edge()
    rg.add_edge(8, "ben", "hamza")
    try:
        wide = rg.edge("ben", "hamza").window(0, 100)
        assert wide.earliest_time == 3
        assert wide.latest_time == 8

        assert wide.shrink_window(0, 5).latest_time == 3
        # shrink_start cuts t=3, keeps t=8.
        assert wide.shrink_start(5).earliest_time == 8
        # shrink_end keeps t=3, cuts t=8.
        assert wide.shrink_end(5).latest_time == 3
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
            assert e.src.edge_history_count() == 1, (
                "expected src().edge_history_count() == 1 under [0,5) window. "
                "If this is 2, the view chain isn't propagating through edges.collect()."
            )

        # Also verify via node → out_edges navigation.
        windowed_out = list(rg.window(0, 5).node("ben").out_edges)
        assert len(windowed_out) == 1
        for e in windowed_out:
            assert e.src.edge_history_count() == 1
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_sorted_by_id():
    """`nodes.sorted([NodeSortBy.by_id()])` returns a nodes collection in
    id order — verified by `.ids()`. `reverse=True` flips it."""
    server_cm, rg = _make_graph_with_edge()
    try:
        asc = rg.nodes.sorted([NodeSortBy.by_id()]).ids()
        assert asc == sorted(asc), f"expected ascending ids, got {asc}"

        desc = rg.nodes.sorted([NodeSortBy.by_id(reverse=True)]).ids()
        assert desc == sorted(desc, reverse=True), (
            f"expected descending ids, got {desc}"
        )
        # Same members, both orderings.
        assert set(asc) == set(desc) == {"ben", "hamza"}
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_sorted_by_property_and_time():
    """Sort by a temporal property and by time. Multi-key lexicographic
    sort — tiebreak on the second key when the first ties."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    try:
        client = server.get_client()
        client.new_graph("g", "EVENT")
        rg = client.remote_graph("g")
        # Three nodes with distinct scores; ben earlier than hamza & zara.
        rg.add_node(1, "ben", properties={"score": 3.0})
        rg.add_node(2, "hamza", properties={"score": 1.0})
        rg.add_node(3, "zara", properties={"score": 2.0})

        by_score = rg.nodes.sorted([NodeSortBy.by_property("score")]).ids()
        assert by_score == ["hamza", "zara", "ben"], (
            f"expected ascending by score: hamza(1), zara(2), ben(3); got {by_score}"
        )

        by_score_desc = rg.nodes.sorted(
            [NodeSortBy.by_property("score", reverse=True)]
        ).ids()
        assert by_score_desc == ["ben", "zara", "hamza"]

        by_earliest = rg.nodes.sorted(
            [NodeSortBy.by_time(SortByTime.EARLIEST)]
        ).ids()
        assert by_earliest == ["ben", "hamza", "zara"]

        by_latest_desc = rg.nodes.sorted(
            [NodeSortBy.by_time(SortByTime.LATEST, reverse=True)]
        ).ids()
        assert by_latest_desc == ["zara", "hamza", "ben"]
    finally:
        server_cm.__exit__(None, None, None)


def test_nodes_sorted_is_lazy_and_composable():
    """`.sorted()` doesn't fire an RPC on its own; it returns a `RemoteNodes`
    that composes with downstream terminals like `.count()` and `.collect()`."""
    server_cm, rg = _make_graph_with_edge()
    try:
        sorted_nodes = rg.nodes.sorted([NodeSortBy.by_id()])
        # Terminal still works — count == 2.
        assert sorted_nodes.count() == 2
        # `.collect()` returns full node handles in sorted order.
        materialized = sorted_nodes.collect()
        assert [n.name for n in materialized] == sorted(
            n.name for n in materialized
        )
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_sorted_by_src_dst():
    """Sort edges by src then dst — lexicographic multi-key."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    try:
        client = server.get_client()
        client.new_graph("g", "EVENT")
        rg = client.remote_graph("g")
        rg.add_edge(1, "b", "c")
        rg.add_edge(2, "a", "c")
        rg.add_edge(3, "a", "b")

        sorted_edges = rg.edges.sorted(
            [EdgeSortBy.by_src(), EdgeSortBy.by_dst()]
        ).collect()
        pairs = [(e.src.name, e.dst.name) for e in sorted_edges]
        assert pairs == [("a", "b"), ("a", "c"), ("b", "c")], (
            f"expected [(a,b),(a,c),(b,c)] by (src, dst), got {pairs}"
        )
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_sorted_by_time_and_property():
    """Sort edges by earliest observed time; also by an edge property."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    try:
        client = server.get_client()
        client.new_graph("g", "EVENT")
        rg = client.remote_graph("g")
        # Three edges at different times, with a weight property.
        rg.add_edge(10, "a", "b", properties={"weight": 2.0})
        rg.add_edge(5, "a", "c", properties={"weight": 3.0})
        rg.add_edge(20, "b", "c", properties={"weight": 1.0})

        by_earliest = rg.edges.sorted(
            [EdgeSortBy.by_time(SortByTime.EARLIEST)]
        ).collect()
        # (a,c)@5, (a,b)@10, (b,c)@20
        pairs = [(e.src.name, e.dst.name) for e in by_earliest]
        assert pairs == [("a", "c"), ("a", "b"), ("b", "c")]

        by_weight_desc = rg.edges.sorted(
            [EdgeSortBy.by_property("weight", reverse=True)]
        ).collect()
        # weights: 3, 2, 1 -> (a,c), (a,b), (b,c)
        pairs = [(e.src.name, e.dst.name) for e in by_weight_desc]
        assert pairs == [("a", "c"), ("a", "b"), ("b", "c")]
    finally:
        server_cm.__exit__(None, None, None)


def test_edges_sorted_composes_with_view_chain():
    """`.sorted()` composes with a windowed view — sort applies only to
    edges visible in the window."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    try:
        client = server.get_client()
        client.new_graph("g", "EVENT")
        rg = client.remote_graph("g")
        rg.add_edge(1, "a", "b")
        rg.add_edge(5, "a", "c")
        rg.add_edge(20, "b", "c")

        windowed_sorted = rg.window(0, 10).edges.sorted(
            [EdgeSortBy.by_time(SortByTime.EARLIEST)]
        ).collect()
        pairs = [(e.src.name, e.dst.name) for e in windowed_sorted]
        # Only the first two edges are in [0, 10). Sorted by earliest time.
        assert pairs == [("a", "b"), ("a", "c")]
    finally:
        server_cm.__exit__(None, None, None)


def _make_shared_neighbours_graph():
    """Two hub nodes (a, d) that share neighbours (b, c) plus a
    non-shared neighbour on each side (e touches only a; f touches only d).
    Shared: {b, c}. Non-shared: e (only a), f (only d)."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    client = server.get_client()
    client.new_graph("g", "EVENT")
    rg = client.remote_graph("g")
    rg.add_edge(1, "a", "b")
    rg.add_edge(2, "a", "c")
    rg.add_edge(3, "a", "e")   # a only
    rg.add_edge(4, "d", "b")
    rg.add_edge(5, "d", "c")
    rg.add_edge(6, "d", "f")   # d only
    return server_cm, rg


def test_shared_neighbours_intersection():
    """`shared_neighbours` returns the intersection of neighbours across
    the input ids."""
    server_cm, rg = _make_shared_neighbours_graph()
    try:
        shared = rg.shared_neighbours(["a", "d"])
        names = sorted(n.name for n in shared)
        assert names == ["b", "c"], f"expected [b, c], got {names}"
    finally:
        server_cm.__exit__(None, None, None)


def test_shared_neighbours_single_node():
    """One input id returns all its neighbours (intersection of one set)."""
    server_cm, rg = _make_shared_neighbours_graph()
    try:
        shared = rg.shared_neighbours(["a"])
        names = sorted(n.name for n in shared)
        assert names == ["b", "c", "e"]
    finally:
        server_cm.__exit__(None, None, None)


def test_shared_neighbours_empty_and_missing():
    """Empty input list → []. Missing ids are silently dropped server-side;
    the intersection is taken over the ids that do exist. All-missing → []."""
    server_cm, rg = _make_shared_neighbours_graph()
    try:
        # Empty input.
        assert rg.shared_neighbours([]) == []

        # `z` doesn't exist and is dropped — result is `a`'s neighbours.
        with_missing = rg.shared_neighbours(["a", "z"])
        names = sorted(n.name for n in with_missing)
        assert names == ["b", "c", "e"]

        # All ids missing → nothing to intersect → [].
        assert rg.shared_neighbours(["x", "y", "z"]) == []
    finally:
        server_cm.__exit__(None, None, None)


def test_shared_neighbours_returns_usable_handles():
    """Returned RemoteNode handles carry the current view chain — terminals
    like `.degree()` and `.properties.get(...)` work against them."""
    server_cm, rg = _make_shared_neighbours_graph()
    try:
        shared = rg.shared_neighbours(["a", "d"])
        for n in shared:
            # Each shared neighbour has degree 2 (connected to both a and d).
            assert n.degree() == 2
    finally:
        server_cm.__exit__(None, None, None)


def test_neighbours_returns_remote_path_from_node():
    """`.neighbours` / `.in_neighbours` / `.out_neighbours` return the new
    `RemotePathFromNode` type (subset of `RemoteNodes` — no `.sorted` or
    `.default_layer`)."""
    from raphtory.graphql import RemotePathFromNode

    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        # All three navigation accessors return the same type.
        assert isinstance(ben.neighbours, RemotePathFromNode)
        assert isinstance(ben.in_neighbours, RemotePathFromNode)
        assert isinstance(ben.out_neighbours, RemotePathFromNode)
    finally:
        server_cm.__exit__(None, None, None)


def test_remote_path_from_node_terminals():
    """Terminals shared with `RemoteNodes` — `ids`, `count`, `list`, and
    native iteration — all work on the new type."""
    server_cm, rg = _make_graph_with_edge()
    try:
        ben = rg.node("ben")
        assert ben.out_neighbours.ids() == ["hamza"]
        assert ben.out_neighbours.count() == 1
        materialized = ben.out_neighbours.collect()
        assert [n.name for n in materialized] == ["hamza"]
        assert [n.name for n in ben.out_neighbours] == ["hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_remote_path_from_node_view_chain_composes():
    """View-chain builders on `RemotePathFromNode` compose lazily. Terminals
    that inspect membership (`ids`, `list`) reflect the narrowed view."""
    server_cm, rg = _make_graph_with_edge()
    try:
        # Add extra edges so the path has multiple members at different times.
        rg.add_edge(8, "ben", "hamza")

        # `.window()` on the path narrows the view — verified via terminals
        # that walk the collection (ids/list).
        narrowed = rg.node("ben").out_neighbours.window(0, 5)
        assert narrowed.ids() == ["hamza"]

        # Verify chaining preserves the type and lazy semantics.
        chained = rg.node("ben").out_neighbours.window(0, 100).layer("_default")
        assert chained.ids() == ["hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_remote_path_from_node_type_filter():
    """`.type_filter(...)` narrows membership; return type is still
    `RemotePathFromNode`."""
    from raphtory.graphql import RemotePathFromNode

    server_cm, rg = _make_graph_with_edge()
    try:
        rg.node("hamza").set_node_type("bot")
        filtered = rg.node("ben").out_neighbours.type_filter(["bot"])
        assert isinstance(filtered, RemotePathFromNode)
        assert filtered.ids() == ["hamza"]

        # Filter to a non-matching type — result should be empty.
        assert rg.node("ben").out_neighbours.type_filter(["human"]).ids() == []
    finally:
        server_cm.__exit__(None, None, None)


def test_remote_path_from_node_lacks_sorted_and_default_layer():
    """`.sorted` and `.default_layer` are not exposed on `RemotePathFromNode`
    because the server's `GqlPathFromNode` doesn't support them."""
    server_cm, rg = _make_graph_with_edge()
    try:
        neighbours = rg.node("ben").out_neighbours
        assert not hasattr(neighbours, "sorted"), (
            "sorted must not be available on RemotePathFromNode"
        )
        assert not hasattr(neighbours, "default_layer"), (
            "default_layer must not be available on RemotePathFromNode"
        )
    finally:
        server_cm.__exit__(None, None, None)


def test_shared_neighbours_composes_with_view_chain():
    """`.shared_neighbours()` runs against the current view chain — the
    intersection uses the neighbours visible under that view."""
    server_cm, rg = _make_shared_neighbours_graph()
    try:
        # Window [0, 4) excludes d-b (t=4), d-c (t=5), d-f (t=6). In that
        # view, only a's edges are visible (t=1,2,3 → b,c,e); d is not
        # present. Server drops `d` (missing in view), intersection is over
        # `a` alone → a's in-view neighbours = {b, c, e}.
        shared_windowed = rg.window(0, 4).shared_neighbours(["a", "d"])
        names = sorted(n.name for n in shared_windowed)
        assert names == ["b", "c", "e"]

        # Under a broader window that includes all edges, both a and d
        # exist and their common neighbours are [b, c].
        shared_all = rg.window(0, 100).shared_neighbours(["a", "d"])
        names = sorted(n.name for n in shared_all)
        assert names == ["b", "c"]
    finally:
        server_cm.__exit__(None, None, None)


def _make_filter_graph():
    """Graph with 4 nodes, distinct properties, for filter tests."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    client = server.get_client()
    client.new_graph("g", "EVENT")
    rg = client.remote_graph("g")
    # Names and a numeric "score" property for filtering.
    rg.add_node(1, "ben", properties={"score": 10.0})
    rg.add_node(2, "hamza", properties={"score": 5.0})
    rg.add_node(3, "alice", properties={"score": 20.0})
    rg.add_node(4, "bob", properties={"score": 15.0})
    return server_cm, rg


def test_select_nodes_by_name_eq():
    """`Node.name() == "ben"` narrows to the single matching node."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        narrowed = rg.nodes.select(Node.name() == "ben").collect()
        assert [n.name for n in narrowed] == ["ben"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_by_name_contains():
    """`Node.name().contains("b")` matches ben and bob."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        narrowed = rg.nodes.select(Node.name().contains("b")).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["ben", "bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_by_property_gt():
    """`Node.property("score") > 12.0` narrows by numeric property."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        narrowed = rg.nodes.select(Node.property("score") > 12.0).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["alice", "bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_and_combinator():
    """`(name contains "b") & (score > 12)` — only bob."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        combined = (Node.name().contains("b")) & (Node.property("score") > 12.0)
        narrowed = rg.nodes.select(combined).collect()
        assert [n.name for n in narrowed] == ["bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_or_combinator():
    """`(name == "ben") | (score < 6)` — ben and hamza."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        combined = (Node.name() == "ben") | (Node.property("score") < 6.0)
        narrowed = rg.nodes.select(combined).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["ben", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_not_combinator():
    """`~(name == "ben")` — everyone but ben."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        narrowed = rg.nodes.select(~(Node.name() == "ben")).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["alice", "bob", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_returns_lazy_handle():
    """`.select()` returns a `RemoteNodes` — terminals (`.count()`,
    `.ids()`, `.collect()`) all work on it."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        narrowed = rg.nodes.select(Node.property("score") >= 10.0)
        assert narrowed.count() == 3
        assert sorted(narrowed.ids()) == ["alice", "ben", "bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_composes_with_view_chain():
    """`.select()` chains with view ops (`.window()`) — both narrow the
    resulting collection."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # Window [0, 3) sees only ben (t=1) and hamza (t=2). Then filter by
        # score > 6 leaves just ben (score=10).
        narrowed = (
            rg.window(0, 3).nodes.select(Node.property("score") > 6.0).collect()
        )
        assert [n.name for n in narrowed] == ["ben"]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_nodes_can_chain():
    """Chained `.select()` calls compose — server applies each in turn."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # First select narrows to names containing "b"; second narrows to
        # score > 12 — only bob remains.
        narrowed = (
            rg.nodes.select(Node.name().contains("b"))
            .select(Node.property("score") > 12.0)
            .collect()
        )
        assert [n.name for n in narrowed] == ["bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_filter_nodes_preserves_membership():
    """`.filter()` on `RemoteNodes` does NOT narrow the current collection —
    the returned collection retains all original members. The filter is
    retained for downstream traversals. Contrast with `.select()`, which
    narrows membership at this step (tested above)."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # `.filter()` preserves current collection membership.
        all_ids = sorted(rg.nodes.filter(Node.name() == "ben").ids())
        assert all_ids == ["alice", "ben", "bob", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


def _make_edge_filter_graph():
    """Graph with 4 edges carrying a numeric "weight" property, for edge
    filter tests."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    client = server.get_client()
    client.new_graph("g", "EVENT")
    rg = client.remote_graph("g")
    rg.add_edge(1, "ben", "hamza", properties={"weight": 10.0})
    rg.add_edge(2, "ben", "alice", properties={"weight": 5.0})
    rg.add_edge(3, "alice", "bob", properties={"weight": 20.0})
    rg.add_edge(4, "bob", "hamza", properties={"weight": 15.0})
    return server_cm, rg


def _edge_pairs(edges):
    """(src, dst) name pairs for a list of RemoteEdge, sorted."""
    return sorted((e.src.name, e.dst.name) for e in edges)


def test_select_edges_by_property_gt():
    """`Edge.property("weight") > 12.0` narrows by numeric property."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        narrowed = rg.edges.select(Edge.property("weight") > 12.0).collect()
        assert _edge_pairs(narrowed) == [("alice", "bob"), ("bob", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_by_src_name():
    """`Edge.src().name() == "ben"` narrows to edges out of ben."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        narrowed = rg.edges.select(Edge.src().name() == "ben").collect()
        assert _edge_pairs(narrowed) == [("ben", "alice"), ("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_by_dst_name():
    """`Edge.dst().name() == "hamza"` narrows to edges into hamza."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        narrowed = rg.edges.select(Edge.dst().name() == "hamza").collect()
        assert _edge_pairs(narrowed) == [("ben", "hamza"), ("bob", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_and_combinator():
    """`(src == "ben") & (weight > 6)` — only ben-hamza (ben-alice has
    weight 5)."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        combined = (Edge.src().name() == "ben") & (Edge.property("weight") > 6.0)
        narrowed = rg.edges.select(combined).collect()
        assert _edge_pairs(narrowed) == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_or_combinator():
    """`(weight > 18) | (src == "ben")` — alice-bob, ben-hamza, ben-alice."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        combined = (Edge.property("weight") > 18.0) | (Edge.src().name() == "ben")
        narrowed = rg.edges.select(combined).collect()
        assert _edge_pairs(narrowed) == [
            ("alice", "bob"),
            ("ben", "alice"),
            ("ben", "hamza"),
        ]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_not_combinator():
    """`~(dst == "hamza")` — every edge not into hamza."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        narrowed = rg.edges.select(~(Edge.dst().name() == "hamza")).collect()
        assert _edge_pairs(narrowed) == [("alice", "bob"), ("ben", "alice")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_returns_lazy_handle():
    """`.select()` returns a `RemoteEdges` — terminals (`.count()`, `.collect()`)
    all work on it."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        narrowed = rg.edges.select(Edge.property("weight") >= 10.0)
        assert narrowed.count() == 3
        assert _edge_pairs(narrowed.collect()) == [
            ("alice", "bob"),
            ("ben", "hamza"),
            ("bob", "hamza"),
        ]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_composes_with_view_chain():
    """`.select()` chains with view ops (`.window()`) — both narrow the
    resulting collection."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        # Window [0, 3) sees only ben-hamza (t=1) and ben-alice (t=2). Then
        # filter by weight > 6 leaves just ben-hamza (weight=10).
        narrowed = (
            rg.window(0, 3).edges.select(Edge.property("weight") > 6.0).collect()
        )
        assert _edge_pairs(narrowed) == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_select_edges_can_chain():
    """Chained `.select()` calls compose — server applies each in turn."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        # First select narrows to edges out of ben; second narrows to
        # weight > 6 — only ben-hamza remains.
        narrowed = (
            rg.edges.select(Edge.src().name() == "ben")
            .select(Edge.property("weight") > 6.0)
            .collect()
        )
        assert _edge_pairs(narrowed) == [("ben", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_filter_edges_preserves_membership():
    """`.filter()` on `RemoteEdges` does NOT narrow the current collection —
    the returned collection retains all original members. The filter is
    retained for downstream traversals. Contrast with `.select()`, which
    narrows membership at this step (tested above)."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        # `.filter()` preserves current collection membership.
        kept = rg.edges.filter(Edge.src().name() == "ben").collect()
        assert _edge_pairs(kept) == [
            ("alice", "bob"),
            ("ben", "alice"),
            ("ben", "hamza"),
            ("bob", "hamza"),
        ]
    finally:
        server_cm.__exit__(None, None, None)


# --- Batch 12: unified .filter() on Graph / Node / PathFromNode -------------


def _make_node_filter_graph():
    """Hub node 'ben' with three out-neighbours carrying a 'score' property,
    for Node.filter / PathFromNode.filter/select tests."""
    work_dir = tempfile.mkdtemp()
    server_cm = GraphServer(work_dir).start()
    server = server_cm.__enter__()
    client = server.get_client()
    client.new_graph("g", "EVENT")
    rg = client.remote_graph("g")
    rg.add_node(1, "ben", properties={"score": 100.0})
    rg.add_node(1, "hamza", properties={"score": 5.0})
    rg.add_node(1, "alice", properties={"score": 20.0})
    rg.add_node(1, "bob", properties={"score": 15.0})
    rg.add_edge(1, "ben", "hamza")
    rg.add_edge(1, "ben", "alice")
    rg.add_edge(1, "ben", "bob")
    return server_cm, rg


def test_graph_filter_dispatches_node_filter():
    """`RemoteGraph.filter(<node filter>)` routes to the server `filterNodes`
    field — matching the local unified `Graph.filter`. Keeps matching nodes."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # score > 12: alice (20) and bob (15); ben (10) and hamza (5) drop.
        filtered = rg.filter(Node.property("score") > 12.0)
        assert sorted(filtered.nodes.ids()) == ["alice", "bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_filter_dispatches_edge_filter():
    """`RemoteGraph.filter(<edge filter>)` routes to the server `filterEdges`
    field. Keeps matching edges; nodes remain even if all their edges drop."""
    from raphtory.filter import Edge

    server_cm, rg = _make_edge_filter_graph()
    try:
        # weight > 12: alice-bob (20) and bob-hamza (15).
        filtered = rg.filter(Edge.property("weight") > 12.0)
        assert _edge_pairs(filtered.edges.collect()) == [
            ("alice", "bob"),
            ("bob", "hamza"),
        ]
    finally:
        server_cm.__exit__(None, None, None)


def test_graph_filter_composes_with_view_chain():
    """`.filter()` composes with a graph-level view op."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # All four nodes are at t=1..4; window [0,3) keeps ben (t=1) and
        # hamza (t=2). Filter score > 6 then leaves only ben (10).
        filtered = rg.window(0, 3).filter(Node.property("score") > 6.0)
        assert sorted(filtered.nodes.ids()) == ["ben"]
    finally:
        server_cm.__exit__(None, None, None)


def test_node_filter_matches():
    """`RemoteNode.filter(<node filter>)` mirrors local `Node.filter` — a
    terminal on a node that matches the filter still resolves."""
    from raphtory.filter import Node

    server_cm, rg = _make_filter_graph()
    try:
        # ben (score=10) matches score > 6; the name terminal still resolves.
        assert rg.node("ben").filter(Node.property("score") > 6.0).name == "ben"
    finally:
        server_cm.__exit__(None, None, None)


def test_node_filter_rejects_edge_filter():
    """Passing an edge filter to `RemoteNode.filter` raises ValueError."""
    import pytest
    from raphtory.filter import Edge

    server_cm, rg = _make_filter_graph()
    try:
        with pytest.raises(ValueError):
            rg.node("ben").filter(Edge.property("weight") > 1.0)
    finally:
        server_cm.__exit__(None, None, None)


def test_path_from_node_select_narrows():
    """`.select()` on a neighbours path narrows membership at this hop."""
    from raphtory.filter import Node

    server_cm, rg = _make_node_filter_graph()
    try:
        # ben's out-neighbours: hamza (5), alice (20), bob (15).
        # select score > 12 → alice, bob.
        narrowed = rg.node("ben").out_neighbours.select(
            Node.property("score") > 12.0
        )
        assert sorted(narrowed.ids()) == ["alice", "bob"]
    finally:
        server_cm.__exit__(None, None, None)


def test_path_from_node_filter_preserves_membership():
    """`.filter()` on a neighbours path preserves membership (propagates to
    downstream traversals instead of narrowing here)."""
    from raphtory.filter import Node

    server_cm, rg = _make_node_filter_graph()
    try:
        kept = rg.node("ben").out_neighbours.filter(Node.property("score") > 12.0)
        assert sorted(kept.ids()) == ["alice", "bob", "hamza"]
    finally:
        server_cm.__exit__(None, None, None)


# --- collection ergonomics: len()/bool() + dict-protocol ---------------------


def test_collection_len_and_bool():
    """`len()` / `bool()` on remote collections map to `.count()`."""
    server_cm, rg = _make_filter_graph()  # 4 nodes, no edges
    try:
        assert len(rg.nodes) == 4
        assert bool(rg.nodes) is True
        # No edges in this graph.
        assert len(rg.edges) == 0
        assert bool(rg.edges) is False
    finally:
        server_cm.__exit__(None, None, None)


def test_path_from_node_len():
    """`len()` on a neighbours path (`RemotePathFromNode`)."""
    server_cm, rg = _make_node_filter_graph()  # ben -> hamza, alice, bob
    try:
        assert len(rg.node("ben").out_neighbours) == 3
        assert bool(rg.node("ben").out_neighbours) is True
    finally:
        server_cm.__exit__(None, None, None)


def test_metadata_dict_protocol():
    """`RemoteMetadata` is dict-like: `md[k]`, `k in md`, `len(md)`,
    `for k in md`, `md.as_dict()`; `md[missing]` raises `KeyError`."""
    import pytest

    server_cm, rg = _make_graph_with_edge()
    rg.node("ben").add_metadata({"role": "admin", "level": 3, "active": True})
    try:
        md = rg.node("ben").metadata
        assert md["role"] == "admin"          # __getitem__ → raw value
        assert md["level"] == 3
        assert md["active"] is True
        assert "role" in md                   # __contains__
        assert "nonexistent" not in md
        assert len(md) == 3                    # __len__
        assert sorted(md) == ["active", "level", "role"]  # __iter__ over keys
        assert md.as_dict() == {"role": "admin", "level": 3, "active": True}
        with pytest.raises(KeyError):          # strict, unlike .get()
            md["nonexistent"]
        assert md.get("nonexistent") is None
    finally:
        server_cm.__exit__(None, None, None)


def test_properties_dict_protocol():
    """`RemoteProperties` is dict-like too; values are the latest temporal
    value under the current view."""
    import pytest

    server_cm, rg = _make_graph_with_edge()
    rg.node("ben").add_updates(5, properties={"score": 2.5})
    try:
        props = rg.node("ben").properties
        assert props["score"] == 2.5
        assert "score" in props
        assert "nonexistent" not in props
        assert len(props) == 1
        assert list(props) == ["score"]
        assert props.as_dict() == {"score": 2.5}
        with pytest.raises(KeyError):
            props["nonexistent"]
    finally:
        server_cm.__exit__(None, None, None)


def test_collection_getitem_is_select():
    """`nodes[filter]` / `edges[filter]` are sugar for `.select(filter)` —
    matching the local API, where `__getitem__` takes a FilterExpr."""
    from raphtory.filter import Edge, Node

    server_cm, rg = _make_filter_graph()  # 4 nodes with a score property
    try:
        # nodes[<node filter>] == nodes.select(<node filter>)
        assert sorted(rg.nodes[Node.property("score") > 12.0].ids()) == ["alice", "bob"]
    finally:
        server_cm.__exit__(None, None, None)

    server_cm, rg = _make_edge_filter_graph()
    try:
        # edges[<edge filter>] == edges.select(<edge filter>)
        got = _edge_pairs(rg.edges[Edge.property("weight") > 12.0].collect())
        assert got == [("alice", "bob"), ("bob", "hamza")]
    finally:
        server_cm.__exit__(None, None, None)


def test_node_edge_getitem_property():
    """`node[key]` / `edge[key]` return the property value. `node[missing]`
    raises `KeyError`; `edge[missing]` returns `None` (matches local)."""
    import pytest

    server_cm, rg = _make_graph_with_edge()
    rg.node("ben").add_updates(5, properties={"score": 2.5})
    rg.add_edge(6, "ben", "hamza", properties={"weight": 9.0})
    try:
        assert rg.node("ben")["score"] == 2.5
        with pytest.raises(KeyError):
            rg.node("ben")["nonexistent"]

        assert rg.edge("ben", "hamza")["weight"] == 9.0
        assert rg.edge("ben", "hamza")["nonexistent"] is None
    finally:
        server_cm.__exit__(None, None, None)
