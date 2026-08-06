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

import contextlib
import tempfile

import pytest

from raphtory.graphql import EdgeSortBy, GraphServer, NodeSortBy, SortByTime


@contextlib.contextmanager
def _remote_graph_and_client(name="g", graph_type="EVENT"):
    """Start a GraphServer in a self-cleaning temp dir, create one graph on
    it, and yield `(RemoteGraph, RaphtoryClient)`.

    The single server fixture every test goes through: population differs per
    test, so callers add their own nodes/edges to the yielded handle. The temp
    dir is removed on exit (`TemporaryDirectory`, not `mkdtemp` — no leaked
    directories), which is safe because the server's `__exit__` joins the
    server task before we get here.
    """
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            client = server.get_client()
            client.new_graph(name, graph_type)
            yield client.remote_graph(name), client


@contextlib.contextmanager
def _remote_graph(name="g", graph_type="EVENT"):
    """As `_remote_graph_and_client`, yielding just the `RemoteGraph` — the
    fixture nearly every test wants."""
    with _remote_graph_and_client(name, graph_type) as (rg, _client):
        yield rg


@contextlib.contextmanager
def _make_graph_with_edge():
    """Yield a RemoteGraph for a graph with two nodes and an edge at t=3.

    A context manager — the server is started on enter and torn down on exit.
    """
    with _remote_graph("test-graph") as rg:
        rg.add_node(1, "ben")
        rg.add_node(2, "hamza")
        rg.add_edge(3, "ben", "hamza")
        yield rg


def test_add_and_degree():
    """Writes and unwindowed reads both route through Transport."""
    with _make_graph_with_edge() as rg:
        assert rg.node("ben").degree() == 1
        assert rg.node("hamza").degree() == 1


def test_view_boundary_semantics():
    """Boundary semantics match the local API exactly: `after(t)` and
    `before(t)` are *exclusive* of `t`, while `at(t)` includes exactly `t`.
    Pinned against an edge whose only event is at exactly the boundary."""
    with _make_graph_with_edge() as rg:
        # _make_graph_with_edge already has ben->hamza at t=3.
        rg.add_edge(10, "x", "y")  # x->y has a single event, exactly at t=10
        # after(t) is strictly after — an event exactly at t is excluded.
        assert rg.after(10).edges.count() == 0
        assert rg.after(9).edges.count() == 1  # only x->y (t=10)
        # at(t) includes exactly t; before(t) excludes it.
        assert rg.at(10).edges.count() == 1  # only x->y
        assert rg.before(10).edges.count() == 1  # only ben->hamza (t=3)


def test_event_id_precise_windowing():
    """A `(timestamp, event_id)` tuple bound windows by the full `EventTime`,
    matching local raphtory: the event id refines the boundary rather than
    being truncated to timestamp precision. Verified against a local twin,
    including through materialization (the event id survives replay onto
    collected members)."""
    from raphtory import Graph

    def build(g):
        # (a,b) and (c,d) share timestamp 5, distinguished only by event_id.
        g.add_edge(5, "a", "b", event_id=0)  # EventTime(5, 0)
        g.add_edge(5, "c", "d", event_id=1)  # EventTime(5, 1)

    local = Graph()
    build(local)

    with _remote_graph("g") as rg:
        build(rg)

        def redges(view):
            return sorted((e.src.name, e.dst.name) for e in view.edges.collect())

        # A bare timestamp (and an event id of 0) keep both same-timestamp edges.
        assert redges(rg.window(5, 10)) == [("a", "b"), ("c", "d")]
        assert redges(rg.window((5, 0), 10)) == [("a", "b"), ("c", "d")]
        # A non-zero event id in the start bound excludes the (5,0) event —
        # exactly what the local twin returns.
        assert redges(rg.window((5, 1), 10)) == [("c", "d")]
        assert redges(rg.window((5, 1), 10)) == sorted(
            (e.src.name, e.dst.name) for e in local.window((5, 1), 10).edges
        )
        # The END bound is event-id precise too (and exclusive): `[0, (5,1))`
        # keeps the (5,0) event but excludes (5,1).
        assert redges(rg.window(0, (5, 1))) == [("a", "b")]
        assert redges(rg.window(0, (5, 1))) == sorted(
            (e.src.name, e.dst.name) for e in local.window(0, (5, 1)).edges
        )
        # Materialization: the event id survives HandleCtx replay onto the
        # collected members (this is the round-trip that a partial fix breaks).
        assert sorted(n.name for n in rg.window((5, 1), 10).nodes.collect()) == [
            "c",
            "d",
        ]


def test_view_ops_accept_str_and_datetime():
    """Remote view ops accept `int | str | datetime` bounds, matching local —
    the conversion is the shared `EventTime` `FromPyObject`. Pins the parity
    claim that was previously untested."""
    import datetime as _dt

    with _make_graph_with_edge() as rg:
        # ben->hamza is at t=3 (ms since epoch → 1970-01-01T00:00:00.003Z).
        assert rg.window("1970-01-01", "2000-01-01").edges.count() == 1  # ISO strings
        aware = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)  # epoch 0
        assert rg.after(aware).edges.count() == 1  # t=3 is after epoch start
        assert rg.before("1970-01-01").edges.count() == 0  # nothing before epoch


def test_add_updates_event_id_precise():
    """`add_updates` carries the `(timestamp, event_id)` secondary index to the
    server (matching local + the read path) rather than truncating it."""
    with _remote_graph_and_client("g") as (rg, client):
        rg.add_node(1, "n")
        rg.node("n").add_updates(5, properties={"p": 1}, event_id=0)
        rg.node("n").add_updates(5, properties={"p": 2}, event_id=1)
        g = client.receive_graph("g")
        # Two updates share t=5 but differ by event_id → both persist.
        assert sorted(et.event_id for et in g.node("n").history if et.t == 5) == [0, 1]


def test_empty_graph_reads():
    """Reads on a graph with no nodes or edges return empties, never errors:
    counts are 0, collections are empty, and the graph's earliest/latest time
    are `None` (not a phantom event time)."""
    with _remote_graph("empty") as rg:
        assert rg.nodes.count() == 0
        assert rg.edges.count() == 0
        assert rg.nodes.collect() == []
        assert rg.edges.collect() == []
        assert rg.earliest_time is None
        assert rg.latest_time is None


def test_event_id_secondary_index():
    """`event_id` disambiguates multiple updates at the same timestamp on
    `add_edge` / `add_node` / `create_node` — parity with the local write API,
    where an explicit event id locks the secondary index instead of
    auto-incrementing."""
    with _remote_graph_and_client("g") as (rg, client):
        # Two edges at the same timestamp with distinct event ids both persist.
        rg.add_edge(1, "a", "b", event_id=0)
        rg.add_edge(1, "a", "c", event_id=1)
        rg.add_node(5, "x", node_type="person", event_id=2)
        rg.create_node(6, "y", node_type="robot", layer="L1", event_id=3)

        g = client.receive_graph("g")
        assert sorted((e.src.name, e.dst.name) for e in g.edges) == [
            ("a", "b"),
            ("a", "c"),
        ]
        assert g.node("x").earliest_time.event_id == 2
        assert g.node("y").earliest_time.event_id == 3
        # create_node's layer argument (new to the client) reached the server.
        assert "L1" in g.unique_layers


def test_windowed_degree():
    """`.window()` composes with `.node().degree()` — RPC is fired only at `.degree()`."""
    with _make_graph_with_edge() as rg:
        # Window [0, 5) includes the edge added at t=3.
        assert rg.window(0, 5).node("ben").degree() == 1
        # Window [0, 2) excludes the edge — ben has no in-window neighbours.
        assert rg.window(0, 2).node("ben").degree() == 0


def test_view_chain_propagation():
    """`PyRemoteGraph.node()` must forward the accumulated view chain into the
    returned `RemoteNode` — otherwise the window is silently dropped and both
    windowed queries collapse to the global degree.
    """
    with _make_graph_with_edge() as rg:
        d_including_edge = rg.window(0, 5).node("ben").degree()
        d_excluding_edge = rg.window(0, 2).node("ben").degree()
        assert d_including_edge != d_excluding_edge, (
            "windowed queries should differ — if they don't, the view chain is "
            "being dropped when descending from RemoteGraph to RemoteNode"
        )


def test_graph_terminals():
    """`count_nodes` / `count_edges` on `RemoteGraph`, both unwindowed and
    under a view chain."""
    with _make_graph_with_edge() as rg:
        assert rg.count_nodes() == 2
        assert rg.count_edges() == 1

        # Window [0, 3) includes ben (t=1) and hamza (t=2) but excludes the
        # edge (added at t=3, and window end is exclusive).
        rg_narrow = rg.window(0, 3)
        assert rg_narrow.count_nodes() == 2
        assert rg_narrow.count_edges() == 0


def test_node_terminals():
    """`.name()`, `.in_degree()`, `.out_degree()` on `RemoteNode`."""
    with _make_graph_with_edge() as rg:
        ben = rg.node("ben")
        assert ben.name == "ben"
        assert ben.out_degree() == 1  # ben → hamza
        assert ben.in_degree() == 0

        hamza = rg.node("hamza")
        assert hamza.out_degree() == 0
        assert hamza.in_degree() == 1  # ben → hamza


def test_view_ops():
    """`.at(...)`, `.before(...)`, `.after(...)` are lazy builders that
    compose with terminals. Server-side `.after` is an exclusive lower bound
    (strictly-after semantics), `.before` is an exclusive upper bound."""
    with _make_graph_with_edge() as rg:
        # `.before(3)` — strictly before t=3 — edge at t=3 not visible.
        assert rg.before(3).node("ben").degree() == 0
        # `.before(4)` — includes the edge at t=3.
        assert rg.before(4).node("ben").degree() == 1
        # `.after(0)` — strictly after t=0 — all events visible.
        assert rg.after(0).node("ben").degree() == 1
        # `.at(3)` snapshots at t=3 — edge exists.
        assert rg.at(3).node("ben").degree() == 1


def test_compound_time_terminals():
    """Compound terminals (`earliest_time`, `latest_time`, `start`, `end`) require
    2-step JSON navigation (`<field> { timestamp }`) and can return `None` when
    the view has no events."""
    with _make_graph_with_edge() as rg:
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
        assert ben.latest_time == 3  # participated in edge at t=3


def test_graph_bool_and_i64_terminals():
    """`has_node`, `has_edge`, `count_temporal_edges` on `RemoteGraph`."""
    with _make_graph_with_edge() as rg:
        assert rg.has_node("ben") is True
        assert rg.has_node("unknown") is False
        assert rg.has_edge("ben", "hamza") is True
        assert rg.has_edge("hamza", "ben") is False  # edges are directed
        # 1 edge added once → 1 temporal edge event.
        assert rg.count_temporal_edges() == 1


def test_node_id_type_and_state():
    """`id`, `node_type`, `is_active`, `edge_history_count` on `RemoteNode`."""
    with _make_graph_with_edge() as rg:
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


def test_snapshot_latest_exclude_shrink_view_ops():
    """`.snapshot_at()`, `.latest()`, `.snapshot_latest()`, `.exclude_layer()`,
    `.shrink_window()`, `.shrink_end()` — all lazy builders that compose with
    terminals."""
    with _make_graph_with_edge() as rg:
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


def test_graph_string_terminals():
    """`.name()`, `.path()`, `.namespace()` on `RemoteGraph`."""
    with _make_graph_with_edge() as rg:
        # We created the graph at path "test-graph" — the leaf name is "test-graph"
        # and the namespace is the empty root.
        assert rg.name() == "test-graph"
        assert rg.path() == "test-graph"
        # Namespace is the parent-path prefix of the graph path. A top-level
        # graph (no "/" in its path) has the empty root namespace.
        assert rg.namespace() == ""


def test_list_arg_view_ops():
    """List-arg view ops: `.layers(...)`, `.exclude_layers(...)`, `.subgraph(...)`,
    `.subgraph_node_types(...)`, `.exclude_nodes(...)`."""
    with _make_graph_with_edge() as rg:
        # `.layers(["_default"])` — restrict to default layer (where our edge lives).
        assert rg.layers(["_default"]).node("ben").degree() == 1
        # `.exclude_layers(["_default"])` — exclude the layer containing the edge.
        assert rg.exclude_layers(["_default"]).node("ben").degree() == 0
        # `.subgraph(["ben"])` — restrict to just the ben node.
        assert rg.subgraph(["ben"]).count_nodes() == 1
        # `.exclude_nodes(["hamza"])` — leaves just ben.
        assert rg.exclude_nodes(["hamza"]).count_nodes() == 1


def test_default_layer_and_valid():
    """`.default_layer()` and `.valid()` are parameterless view builders."""
    with _make_graph_with_edge() as rg:
        # `.default_layer()` restricts to the default layer — edge is on it.
        assert rg.default_layer().node("ben").degree() == 1
        # `.valid()` filters out invalid entities. On an event graph with only
        # add ops, this is a no-op — count matches unwindowed.
        assert rg.valid().count_nodes() == 2


def test_nodes_collection():
    """`rg.nodes` accessor returns a `RemoteNodes` collection with `.id`,
    `.count()`, and `.collect()` terminals."""
    with _make_graph_with_edge() as rg:
        nodes = rg.nodes
        assert nodes.count() == 2
        assert sorted(nodes.id) == ["ben", "hamza"]

        # Materialize as RemoteNode handles, then read a scalar off each.
        remote_nodes = nodes.collect()
        assert len(remote_nodes) == 2
        names = sorted(n.name for n in remote_nodes)
        assert names == ["ben", "hamza"]


def test_view_chain_propagates_through_collection_list():
    """Regression: previously `rg.window(...).nodes.collect()` rebased returned
    nodes at Root, causing view-dependent terminals to silently give wrong
    answers. After the base_graph fix, materialized nodes carry the parent
    view forward.
    """
    with _make_graph_with_edge() as rg:
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


def test_nodes_native_iteration():
    """`for n in rg.nodes:` — no explicit `.collect()` needed."""
    with _make_graph_with_edge() as rg:
        names = sorted(n.name for n in rg.nodes)
        assert names == ["ben", "hamza"]

        # Native iteration over a navigation collection.
        out_names = [n.name for n in rg.node("ben").out_neighbours]
        assert out_names == ["hamza"]

        # Iterating twice is idempotent (each iter() call fetches fresh).
        first = [n.name for n in rg.nodes]
        second = [n.name for n in rg.nodes]
        assert sorted(first) == sorted(second)


def test_node_neighbour_collections():
    """`.neighbours`, `.in_neighbours`, `.out_neighbours` on `RemoteNode`."""
    with _make_graph_with_edge() as rg:
        ben = rg.node("ben")
        # ben has one out-neighbour (hamza) and zero in-neighbours.
        assert ben.out_neighbours.id == ["hamza"]
        assert ben.in_neighbours.id == []
        # `.neighbours` is directed union — includes hamza.
        assert ben.neighbours.id == ["hamza"]

        hamza = rg.node("hamza")
        assert hamza.in_neighbours.id == ["ben"]
        assert hamza.out_neighbours.id == []


def test_edge_selection_and_navigation():
    """`rg.edge(src, dst)` selects an edge; `.src()` / `.dst()` navigate back
    to node handles that carry the whole view chain."""
    with _make_graph_with_edge() as rg:
        e = rg.edge("ben", "hamza")
        # Navigate back to source/destination nodes and read from them.
        assert e.src.name == "ben"
        assert e.dst.name == "hamza"
        # The navigated-back node handles carry the full view chain — evaluating
        # a terminal on them fires an RPC against the same underlying edge.
        assert e.src.degree() == 1
        assert e.dst.degree() == 1


def test_edges_collection():
    """`rg.edges` accessor returns a `RemoteEdges` collection with `.count()`
    and `.collect()` terminals. Edge ids are `(src, dst)` pairs, via `.id`."""
    with _make_graph_with_edge() as rg:
        edges = rg.edges
        assert edges.count() == 1

        # Materialize as RemoteEdge handles; navigate back to endpoints.
        remote_edges = edges.collect()
        assert len(remote_edges) == 1
        pairs = sorted((e.src.name, e.dst.name) for e in remote_edges)
        assert pairs == [("ben", "hamza")]


def test_edges_native_iteration():
    """`for e in rg.edges:` yields `RemoteEdge` handles without an explicit
    `.collect()` call."""
    with _make_graph_with_edge() as rg:
        # Add a second edge so we can verify multi-edge iteration.
        rg.add_node(4, "sam")
        rg.add_edge(5, "ben", "sam")
        pairs = sorted((e.src.name, e.dst.name) for e in rg.edges)
        assert pairs == [("ben", "hamza"), ("ben", "sam")]

        # Native iteration over a node's out_edges collection.
        out_pairs = sorted((e.src.name, e.dst.name) for e in rg.node("ben").out_edges)
        assert out_pairs == [("ben", "hamza"), ("ben", "sam")]


def test_node_edge_collections():
    """`.edges`, `.in_edges`, `.out_edges` on `RemoteNode`."""
    with _make_graph_with_edge() as rg:
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


def test_graph_metadata_timestamps():
    """`created`, `last_opened`, `last_updated` on the graph return non-null
    system timestamps (wall-clock ms, set by the server when the graph is
    saved/opened/updated on disk)."""
    with _make_graph_with_edge() as rg:
        created = rg.created()
        last_opened = rg.last_opened()
        last_updated = rg.last_updated()
        # All three are non-null wall-clock milliseconds — must be positive.
        assert created > 0
        assert last_opened > 0
        assert last_updated > 0
        # Sanity: last_updated must be at or after created.
        assert last_updated >= created


def test_graph_edge_time_terminals():
    """`earliest_edge_time` / `latest_edge_time` return event timestamps under
    the current view. Nullable — empty view returns None."""
    with _make_graph_with_edge() as rg:
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


def test_node_update_time_terminals():
    """`first_update` / `last_update` on a node return the range of event
    timestamps that touched this node under the current view."""
    with _make_graph_with_edge() as rg:
        # ben has add_node at t=1 and add_edge (ben, hamza) at t=3.
        ben = rg.node("ben")
        assert ben.first_update() == 1
        assert ben.last_update() == 3

        # Windowed view narrows the range — only the t=3 edge event visible.
        ben_windowed = rg.window(2, 5).node("ben")
        assert ben_windowed.first_update() == 3
        assert ben_windowed.last_update() == 3


def test_absent_node_or_edge_returns_none():
    """`.node()` / `.edge()` return `None` when the id isn't present in the
    current view — matching the local `Graph.node -> Optional[Node]` — rather
    than raising. Covers both absent-from-graph and absent-from-window; the
    server can't distinguish the two, so both collapse to `None`."""
    with _make_graph_with_edge() as rg:
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


def test_node_view_chain_builders():
    """RemoteNode has full view-chain builder parity with the local Node —
    `.window`, `.at`, `.before`, `.after`, `.latest`, `.snapshot_at`,
    `.snapshot_latest`, `.shrink_*`, `.default_layer`, `.layer`, `.layers`,
    `.exclude_layer`, `.exclude_layers`. All lazy — no RPC until a terminal."""
    with _make_graph_with_edge() as rg:
        # Add a second edge event on the same pair at t=8 so we can distinguish
        # windowed views clearly.
        rg.add_edge(8, "ben", "hamza")
        ben = rg.node("ben")

        # Global vs windowed on the same node handle.
        assert ben.edge_history_count() == 2  # two edge events total
        assert ben.window(0, 5).edge_history_count() == 1
        assert ben.window(6, 10).edge_history_count() == 1
        assert ben.window(100, 200).edge_history_count() == 0

        # At — snapshot at a specific time.
        assert ben.at(3).is_active() is True
        assert ben.at(5).is_active() is False  # window [5, 6) — no events

        # Before / after — one-sided views.
        assert ben.before(5).edge_history_count() == 1  # only t=3
        assert ben.after(5).edge_history_count() == 1  # only t=8
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
        neighbours = ben.window(0, 5).out_neighbours.id
        assert neighbours == ["hamza"]
        assert ben.window(100, 200).out_neighbours.count() == 0

        # Chain after selection order commutes with pre-selection.
        assert ben.window(0, 5).degree() == rg.window(0, 5).node("ben").degree()


def test_node_shrink_builders():
    """`.shrink_window`, `.shrink_start`, `.shrink_end` narrow an existing window."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")
        # Start from a wide window, then shrink it.
        wide = rg.node("ben").window(0, 100)
        assert wide.edge_history_count() == 2

        # Shrink both ends.
        assert wide.shrink_window(0, 5).edge_history_count() == 1
        # Shrink start only — cuts off t=3, keeps t=8.
        assert wide.shrink_start(5).edge_history_count() == 1
        # Shrink end only — keeps t=3, cuts off t=8.
        assert wide.shrink_end(5).edge_history_count() == 1


def test_edge_read_terminals():
    """Read terminals on RemoteEdge — time, layer, id, bool state — mirror
    the shape of the Node terminals under the current view."""
    with _make_graph_with_edge() as rg:
        # Second edge event on the same pair at t=8, so we can distinguish
        # first_update vs last_update on the edge itself.
        rg.add_edge(8, "ben", "hamza")
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


def test_edge_self_loop_and_absent():
    """`is_self_loop` returns True for src == dst; absent edges return None."""
    with _make_graph_with_edge() as rg:
        # A self-loop edge.
        rg.add_edge(4, "ben", "ben")
        assert rg.edge("ben", "ben").is_self_loop() is True
        assert rg.edge("ben", "hamza").is_self_loop() is False

        # Absent edge → None (not an error).
        assert rg.edge("nonexistent", "hamza") is None


def test_edge_nbr_navigation():
    """`.nbr()` navigates to the "other end" node; on a plain edge it's
    equivalent to `.dst()`."""
    with _make_graph_with_edge() as rg:
        e = rg.edge("ben", "hamza")
        # On a plain (out-)edge view, nbr yields the destination.
        assert e.nbr.name == "hamza"


def test_collection_view_chain_builders():
    """RemoteNodes and RemoteEdges have full view-chain builder parity with
    the parent Graph — `.window`, `.at`, `.before`, `.after`, `.latest`,
    `.snapshot_at`, `.snapshot_latest`, `.shrink_*`, `.default_layer`,
    `.layer`, `.layers`, `.exclude_layer`, `.exclude_layers`. All lazy."""
    with _make_graph_with_edge() as rg:
        # Add a second edge event to distinguish windowed views clearly.
        rg.add_edge(8, "ben", "hamza")
        # Collection membership is "sticky" — narrowing the view of an already-
        # materialized `.nodes` / `.edges` handle doesn't change its count.
        # Contrast with pre-selection (`rg.window(...).nodes`) where the graph-
        # level view filters membership. Same semantics as node/edge selection.
        assert rg.nodes.window(0, 5).count() == 2
        assert rg.nodes.window(100, 200).count() == 2  # sticky!
        assert rg.window(100, 200).nodes.count() == 0  # graph-level filters
        # Same story on edges — collection membership sticks; view narrows.
        assert rg.edges.window(0, 5).count() == 1
        assert rg.edges.window(100, 200).count() == 1  # sticky
        assert rg.window(100, 200).edges.count() == 0  # graph-level filters

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


def test_collection_view_chain_composes_with_materialization():
    """Materialized handles from a view-narrowed collection carry the view
    forward — tests `base_graph` propagation through view builders on the
    collection. `for n in ...:` uses `__iter__` which delegates to `.collect()`;
    both paths hit the same base_graph plumbing."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")
        # Iterate over a window-narrowed collection — each yielded handle
        # should see the windowed view.
        for n in rg.nodes.window(0, 5):
            if n.name == "ben":
                # Only the t=3 edge is visible in [0, 5) — ben's history count is 1.
                assert n.edge_history_count() == 1


def test_node_view_chain_propagates_through_neighbour_materialization():
    """Regression for the same `base_graph` bug — but on `RemoteNode`. If
    view builders on Node don't update `base_graph`, then materialized
    neighbours would revert to the unwindowed graph view."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")
        # Take ben, narrow to [0, 5), then materialize his out_neighbours.
        # Each neighbour should still see the windowed view — meaning
        # hamza's edge_history_count under that view is 1, not 2.
        for n in rg.node("ben").window(0, 5).out_neighbours:
            assert n.name == "hamza"
            assert n.edge_history_count() == 1, (
                "expected 1 under [0,5) window. If this is 2, base_graph is "
                "not propagating through RemoteNode's view builders."
            )


def test_history_scalar_terminals_on_node():
    """`node.history` returns a `RemoteHistory` container with scalar
    terminals — `count`, `is_empty`, `earliest_time`, `latest_time`. Access
    is via property (matching local API), not method."""
    with _make_graph_with_edge() as rg:
        # Node ben: add_node at t=1, add_edge (ben, hamza) at t=3 → 2 events.
        h = rg.node("ben").history  # property, not method
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


def test_history_scalar_terminals_on_edge():
    """`edge.history` and `edge.deletions` — both return `RemoteHistory`
    handles but read different server fields."""
    with _make_graph_with_edge() as rg:
        # Edge (ben, hamza): one event at t=3, no deletions.
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


def test_history_list_and_iter():
    """`history.collect()` returns `List[RemoteEventTime]` sorted ascending by
    time; `.collect_rev()` returns them descending. `for t in history:` iterates
    via `__iter__` which delegates to `.collect()`."""
    with _make_graph_with_edge() as rg:
        # ben has events at t=1 (add_node) and t=3 (add_edge). Add another at t=8.
        rg.add_edge(8, "ben", "hamza")
        h = rg.node("ben").history
        events = h.collect()
        assert len(events) == 3
        # Extract timestamps — dt/event_id are also populated but shape-check
        # them separately below.
        assert [e.t for e in events] == [1, 3, 8]

        # list_rev
        events_rev = h.collect_rev()
        assert [e.t for e in events_rev] == [8, 3, 1]

        # Iterator delegates to .collect() — same order.
        via_iter = [e.t for e in h]
        assert via_iter == [1, 3, 8]

        # All three fields populated by the server. dt is a real datetime.
        import datetime as _dt

        for e in events:
            assert e.t is not None
            assert e.event_id is not None
            assert isinstance(e.dt, _dt.datetime)


def test_history_list_on_empty_view():
    """`.collect()` on an empty history returns an empty list, not None."""
    with _make_graph_with_edge() as rg:
        empty = rg.node("ben").window(100, 200).history
        assert empty.collect() == []
        assert empty.collect_rev() == []
        assert list(empty) == []  # iteration also empty


def test_graph_schema():
    """`rg.schema()` fires one RPC and returns the full schema tree —
    node types + edge layers + their property/metadata schemas."""
    with _make_graph_with_edge() as rg:
        # Node types + temporal properties + metadata to make the schema
        # interesting.
        rg.node("ben").set_node_type("user")
        rg.node("hamza").set_node_type("bot")
        rg.node("ben").add_updates(5, properties={"score": 1.5})
        rg.node("ben").add_metadata({"role": "admin"})
        rg.edge("ben", "hamza").add_metadata({"weight": 0.5})
        schema = rg.schema()

        # nodes: one entry per node type
        node_types = sorted(n.type_name for n in schema.nodes)
        assert "user" in node_types
        assert "bot" in node_types

        # user node type has a "score" temporal property
        user_schema = next(n for n in schema.nodes if n.type_name == "user")
        score_prop = next((p for p in user_schema.properties if p.key == "score"), None)
        assert score_prop is not None
        assert score_prop.property_type == "F64"  # the float 1.5 above

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
        weight_meta = next((p for p in edge_schema.metadata if p.key == "weight"), None)
        assert weight_meta is not None


def test_temporal_property_stats():
    """`RemoteTemporalProperty` numeric stats: sum, mean, average, min, max,
    median. Non-numeric aggregates return None. Non-numeric stats return
    `RemotePropertyTuple` with a time and native-Python value."""
    with _make_graph_with_edge() as rg:
        # Numeric values: 1, 2, 3, 4, 5
        for i, t in enumerate([1, 2, 3, 4, 5]):
            rg.node("ben").add_updates(t, properties={"score": float(i + 1)})
        score = rg.node("ben").properties.temporal.get("score")

        # Numeric aggregates on floats: sum=15, mean=3.0, average=3.0
        assert score.sum() == 15.0
        assert score.mean() == 3.0
        assert score.average() == 3.0

        # Min/max/median return RemotePropertyTuple (time + value)
        mn = score.min()
        assert mn is not None
        assert mn.value == 1.0
        assert mn.time.t == 1

        mx = score.max()
        assert mx is not None
        assert mx.value == 5.0
        assert mx.time.t == 5

        med = score.median()
        assert med is not None
        assert med.value == 3.0
        assert med.time.t == 3


def test_temporal_property_unique_and_dedupe():
    """`.unique()` returns distinct values; `.ordered_dedupe(latest_time)`
    collapses runs of consecutive-equal values."""
    with _make_graph_with_edge() as rg:
        # Runs of equal values: 1, 1, 2, 2, 2, 3, 1
        for t, v in [(1, 1), (2, 1), (3, 2), (4, 2), (5, 2), (6, 3), (7, 1)]:
            rg.node("ben").add_updates(t, properties={"status": v})
        status = rg.node("ben").properties.temporal.get("status")

        # Distinct values — order not guaranteed
        assert sorted(status.unique()) == [1, 2, 3]

        # ordered_dedupe(latest_time=False): (1, 1), (3, 2), (6, 3), (7, 1) — first
        # timestamp of each run.
        first_ts = status.ordered_dedupe(latest_time=False)
        assert [(p.time.t, p.value) for p in first_ts] == [
            (1, 1),
            (3, 2),
            (6, 3),
            (7, 1),
        ]

        # ordered_dedupe(latest_time=True): (2, 1), (5, 2), (6, 3), (7, 1) — last
        # timestamp of each run.
        last_ts = status.ordered_dedupe(latest_time=True)
        assert [(p.time.t, p.value) for p in last_ts] == [
            (2, 1),
            (5, 2),
            (6, 3),
            (7, 1),
        ]


def test_temporal_properties_container():
    """`properties.temporal` returns a `RemoteTemporalProperties` container.
    `.get(key)` returns a `RemoteTemporalProperty` handle if present, `None`
    otherwise. `.values()` returns handles for every temporal property."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        tp = rg.node("ben").properties.temporal

        # keys
        assert sorted(tp.keys()) == ["active", "score"]

        # membership via `in` (local uses the operator, not a .contains method)
        assert "score" in tp
        assert "nonexistent" not in tp

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


def test_temporal_properties_histories():
    """`.temporal.histories()` returns `{key: [(EventTime, value), ...]}` for
    every temporal property — mirrors local `TemporalProperties.histories`."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 1.5})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        hs = rg.node("ben").properties.temporal.histories()
        assert isinstance(hs, dict)
        assert "score" in hs
        # score's history: (t=5 → 1.5), (t=10 → 2.5); each entry is
        # (RemoteEventTime, value) and the EventTime compares to its int ts.
        score_hist = hs["score"]
        assert [(t.t, v) for t, v in score_hist] == [(5, 1.5), (10, 2.5)]
        # consistency: histories()[k] == get(k).items()
        assert score_hist == rg.node("ben").properties.temporal.get("score").items()


def test_temporal_property_terminals():
    """`RemoteTemporalProperty` core methods: `.history`, `.values()`,
    `.at(t)`, `.latest()`, `.count()`."""
    with _make_graph_with_edge() as rg:
        # score: 1.5 at t=5, 2.5 at t=10, 3.5 at t=15
        rg.node("ben").add_updates(5, properties={"score": 1.5})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        rg.node("ben").add_updates(15, properties={"score": 3.5})
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
        assert score.at(7) == 1.5  # no update at 7 → latest before is at t=5
        assert score.at(10) == 2.5
        assert score.at(100) == 3.5  # latest before 100 is 3.5

        # at(t) before any update — None
        assert score.at(0) is None

        # history — reuses RemoteHistory
        hist = score.history
        assert hist.count() == 3
        assert hist.collect()[0].t == 5


def test_node_properties_basic():
    """`node.properties` returns a `RemoteProperties` container (temporal +
    metadata). Same terminal shape as metadata; for temporal properties,
    `.get(key)` and `.values()` return the property's most recent value."""
    with _make_graph_with_edge() as rg:
        # Add temporal properties at t=5, t=10.
        rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        props = rg.node("ben").properties

        # keys — all temporal property names.
        assert sorted(props.keys()) == ["active", "score"]

        # membership via `in` (local uses the operator, not a .contains method)
        assert "score" in props
        assert "nonexistent" not in props

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


def test_properties_vs_metadata_separation():
    """`.properties` covers temporal properties; `.metadata` covers non-
    temporal. Server exposes them as separate containers — no overlap in
    keys."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_metadata({"role": "admin"})  # non-temporal
        rg.node("ben").add_updates(5, properties={"score": 1.0})  # temporal
        # Metadata has "role", properties has "score" — no cross-contamination.
        assert rg.node("ben").metadata.keys() == ["role"]
        assert rg.node("ben").properties.keys() == ["score"]

        # get() on the wrong container returns None.
        assert rg.node("ben").metadata.get("score") is None
        assert rg.node("ben").properties.get("role") is None


def test_node_metadata_basic():
    """`node.metadata` returns a `RemoteMetadata` container. Standard shape:
    `get(key)`, `contains(key)`, `keys()`, `values(keys=None)`. Values are
    native Python types via raphtory's Prop → Python conversion."""
    with _make_graph_with_edge() as rg:
        # Attach metadata to ben (non-temporal).
        rg.node("ben").add_metadata({"role": "admin", "level": 3, "active": True})
        md = rg.node("ben").metadata

        # keys — all names present.
        assert sorted(md.keys()) == ["active", "level", "role"]

        # membership via `in` (local uses the operator, not a .contains method)
        assert "role" in md
        assert "nonexistent" not in md

        # get — Optional[RemoteProperty], value is native Python type.
        role = md.get("role")
        assert role is not None
        assert role == "admin"

        level = md.get("level")
        assert level == 3  # int
        active = md.get("active")
        assert active is True  # bool

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


def test_graph_and_edge_metadata():
    """`.metadata` accessor exists on RemoteGraph, RemoteNode, and RemoteEdge
    — same container shape."""
    with _make_graph_with_edge() as rg:
        rg.add_metadata({"description": "test graph"})
        rg.edge("ben", "hamza").add_metadata({"weight": 5.5})
        # Graph metadata
        assert rg.metadata.get("description") == "test graph"

        # Edge metadata
        weight = rg.edge("ben", "hamza").metadata.get("weight")
        assert weight is not None
        assert weight == 5.5


def test_edge_explode():
    """`.explode()` on a `RemoteEdge` fans it out into one entry per event,
    returning a `RemoteEdges` collection. `explode_layers()` fans out by layer."""
    with _make_graph_with_edge() as rg:
        # Add multiple events on the same edge.
        rg.add_edge(5, "ben", "hamza")
        rg.add_edge(8, "ben", "hamza")
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


def test_edges_explode():
    """`.explode()` on a `RemoteEdges` collection expands each member into
    its events. Terminal count reflects the sum of per-edge event counts."""
    with _make_graph_with_edge() as rg:
        # Two edges, ben->hamza with events at t=3 and t=5, ben->sam with event at t=7.
        rg.add_edge(5, "ben", "hamza")
        rg.add_node(6, "sam")
        rg.add_edge(7, "ben", "sam")
        # Total events across both edges: 2 + 1 = 3.
        exploded = rg.edges.explode()
        assert exploded.count() == 3


def test_node_in_out_component():
    """`.in_component` / `.out_component` return the set of ancestors /
    descendants reachable via directed edges (excluding self). Both are
    `RemoteNodes` handles with the usual terminals (count, ids, list, iter)."""
    with _make_graph_with_edge() as rg:
        # Build a chain: ben -> hamza -> sam -> tom  (t=3 already has ben->hamza)
        rg.add_node(4, "sam")
        rg.add_node(5, "tom")
        rg.add_edge(4, "hamza", "sam")
        rg.add_edge(5, "sam", "tom")
        # Out-component from ben: {hamza, sam, tom} (descendants, excludes ben).
        out = rg.node("ben").out_component
        assert sorted(out.id) == ["hamza", "sam", "tom"]
        assert out.count() == 3

        # In-component of tom: {ben, hamza, sam}.
        into_tom = rg.node("tom").in_component
        assert sorted(into_tom.id) == ["ben", "hamza", "sam"]

        # Sam sits in the middle — in-component {ben, hamza}, out-component {tom}.
        assert sorted(rg.node("sam").in_component.id) == ["ben", "hamza"]
        assert rg.node("sam").out_component.id == ["tom"]

        # Terminal node in out-direction: tom's out-component is empty.
        assert rg.node("tom").out_component.id == []
        assert rg.node("tom").out_component.count() == 0

        # Composes with view — under a window that only sees ben->hamza,
        # ben's out-component shrinks to {hamza}.
        windowed = rg.window(0, 4).node("ben").out_component
        assert sorted(windowed.id) == ["hamza"]

        # Iteration works.
        names = sorted(n.name for n in rg.node("ben").out_component)
        assert names == ["hamza", "sam", "tom"]


def test_nodes_type_filter():
    """`rg.nodes.type_filter(types)` filters membership — the returned
    collection has fewer members. Distinct from view ops (window/layer/etc.)
    which are sticky and preserve membership."""
    with _make_graph_with_edge() as rg:
        # Give the nodes distinct types.
        rg.node("ben").set_node_type("user")
        rg.node("hamza").set_node_type("bot")
        # Add a third node with no type.
        rg.add_node(4, "sam")
        all_nodes = rg.nodes
        assert all_nodes.count() == 3

        # Filter to only "user" nodes.
        users = all_nodes.type_filter(["user"])
        assert users.count() == 1
        assert users.id == ["ben"]

        # Filter to multiple types.
        both = all_nodes.type_filter(["user", "bot"])
        assert both.count() == 2
        assert sorted(both.id) == ["ben", "hamza"]

        # Filter to nonexistent type — empty collection.
        empty = all_nodes.type_filter(["nonexistent"])
        assert empty.count() == 0
        assert empty.id == []

        # Filter is composable — narrow further by a window.
        assert all_nodes.type_filter(["user"]).window(0, 5).count() == 1


def test_nodes_type_filter_with_windowed_view():
    """`type_filter` composes with view ops in any order — window then filter,
    filter then window, or graph-level window then nodes then filter."""
    with _make_graph_with_edge() as rg:
        # ben (t=1) and hamza (t=2) are "user"; sam (t=10) is "user" but only
        # appears in the view after t=10.
        rg.add_node(10, "sam")
        rg.node("ben").set_node_type("user")
        rg.node("hamza").set_node_type("bot")
        rg.node("sam").set_node_type("user")
        # (a) Graph-scope window pre-selection → nodes filters membership by
        # window; then type_filter filters by type. Only ben matches "user"
        # in [0, 5) window.
        pre_windowed = rg.window(0, 5).nodes.type_filter(["user"])
        assert pre_windowed.count() == 1
        assert pre_windowed.id == ["ben"]

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


def test_history_sub_containers():
    """`history.t`, `.dt`, `.event_id`, `.intervals` — four
    parallel projections of the same events. Timestamps/event_id/intervals
    return `list[int]`; datetimes return `list[datetime]`, matching the local
    `History.dt`."""
    import datetime as _dt

    with _make_graph_with_edge() as rg:
        # ben events: add_node t=1, add_edge t=3. Add more so intervals are non-trivial.
        rg.add_edge(5, "ben", "hamza")
        rg.add_edge(9, "ben", "hamza")
        h = rg.node("ben").history

        # Timestamps view — plain ints
        assert h.t.collect() == [1, 3, 5, 9]
        assert h.t.collect_rev() == [9, 5, 3, 1]

        # DateTimes view — real datetimes, positionally aligned with timestamps
        # (local parity: not RFC 3339 strings).
        dts = h.dt.collect()
        assert len(dts) == 4
        assert all(isinstance(d, _dt.datetime) for d in dts)
        # t=1 ms since epoch -> 1970-01-01T00:00:00.001Z
        assert dts[0] == _dt.datetime(
            1970, 1, 1, 0, 0, 0, 1000, tzinfo=_dt.timezone.utc
        )

        # Event IDs view — plain ints; server picks per-timestamp
        eids = h.event_id.collect()
        assert len(eids) == 4

        # Intervals view — deltas between consecutive events: 3-1=2, 5-3=2, 9-5=4
        intervals = h.intervals.collect()
        assert intervals == [2, 2, 4]


def test_intervals_stats():
    """`intervals.mean()`, `.median()`, `.max()`, `.min()` — summary stats
    over inter-event gaps."""
    with _make_graph_with_edge() as rg:
        # ben events: t=1, t=3. Add more to make intervals meaningful: [2, 2, 4].
        rg.add_edge(5, "ben", "hamza")
        rg.add_edge(9, "ben", "hamza")
        stats = rg.node("ben").history.intervals

        # intervals = [2, 2, 4], mean = 8/3 ≈ 2.666...
        mean = stats.mean()
        assert mean is not None
        assert abs(mean - 8.0 / 3.0) < 1e-9

        assert stats.median() == 2
        assert stats.max() == 4
        assert stats.min() == 2


def test_sub_container_paging():
    """Sub-containers share the same `page(limit, offset, page_index)` shape
    as the root `RemoteHistory`."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(5, "ben", "hamza")
        rg.add_edge(7, "ben", "hamza")
        rg.add_edge(9, "ben", "hamza")
        ts = rg.node("ben").history.t
        # Full events: [1, 3, 5, 7, 9]
        assert ts.collect() == [1, 3, 5, 7, 9]
        assert ts.page(limit=2) == [1, 3]
        assert ts.page(limit=2, offset=2) == [5, 7]
        assert ts.page(limit=2, page_index=1) == [5, 7]  # equivalent
        assert ts.page_rev(limit=2) == [9, 7]


def test_history_page_and_page_rev():
    """`history.page(limit, offset, page_index)` returns a slice of events;
    `.page_rev(...)` returns the equivalent slice in descending order.
    `offset` and `page_index` default to 0."""
    with _make_graph_with_edge() as rg:
        # Add extra edges so ben has 5 events total: add_node at t=1, edges at
        # t=3, t=5, t=7, t=9.
        rg.add_edge(5, "ben", "hamza")
        rg.add_edge(7, "ben", "hamza")
        rg.add_edge(9, "ben", "hamza")
        h = rg.node("ben").history
        assert h.count() == 5

        # Full first page — limit=2, no offset, no page_index.
        page = h.page(limit=2)
        assert [e.t for e in page] == [1, 3]

        # Explicit offset — skip 2, take 2.
        page_off = h.page(limit=2, offset=2)
        assert [e.t for e in page_off] == [5, 7]

        # page_index=1 with limit=2 → skip 2, take 2 (equivalent to offset=2).
        page_idx = h.page(limit=2, page_index=1)
        assert [e.t for e in page_idx] == [5, 7]

        # page_index=1 with limit=2 AND offset=1 → skip 2+1=3, take 2.
        page_combo = h.page(limit=2, offset=1, page_index=1)
        assert [e.t for e in page_combo] == [7, 9]

        # Limit exceeds remaining — returns whatever is left.
        page_last = h.page(limit=10, offset=3)
        assert [e.t for e in page_last] == [7, 9]

        # Reverse — first page in descending order.
        page_rev = h.page_rev(limit=2)
        assert [e.t for e in page_rev] == [9, 7]

        # Reverse with offset.
        page_rev_off = h.page_rev(limit=2, offset=1)
        assert [e.t for e in page_rev_off] == [7, 5]


def test_edge_history_and_deletions_lists():
    """Edge history and deletions both expose `.collect()` returning
    `RemoteEventTime`s under the same shape."""
    with _make_graph_with_edge() as rg:
        # Add a deletion event at t=10.
        rg.delete_edge(10, "ben", "hamza")
        e = rg.edge("ben", "hamza")

        # Deletions has exactly one entry at t=10.
        deletion_events = e.deletions.collect()
        assert len(deletion_events) == 1
        assert deletion_events[0].t == 10

        # History exposes non-deletion events.
        history_events = e.history.collect()
        assert len(history_events) >= 1
        assert all(ev.t is not None for ev in history_events)


def test_history_records_deletion_event():
    """After `.delete_edge()`, the edge's `.deletions` history includes the
    deletion time; `.history` reflects the add event."""
    with _make_graph_with_edge() as rg:
        # Delete the ben→hamza edge at t=10.
        rg.delete_edge(10, "ben", "hamza")

        e = rg.edge("ben", "hamza")
        assert e.deletions.count() == 1
        assert e.deletions.earliest_time() == 10


def test_collection_view_bounds():
    """`.start()` / `.end()` on RemoteNodes and RemoteEdges report the
    inherited view bound. `None` when the parent view is unbounded, matching
    the semantics on Graph / Node / Edge."""
    with _make_graph_with_edge() as rg:
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


def test_graph_unique_layers():
    """`unique_layers` returns the list of layer names present in the graph."""
    with _make_graph_with_edge() as rg:
        assert rg.unique_layers == ["_default"]

        # Add an edge on a distinct layer.
        rg.add_edge(4, "ben", "hamza", layer="secret")
        # Now two layers are present.
        assert sorted(rg.unique_layers) == ["_default", "secret"]


def test_edge_view_chain_builders():
    """RemoteEdge has full view-chain builder parity with the local Edge —
    `.window`, `.at`, `.before`, `.after`, `.latest`, `.snapshot_at`,
    `.snapshot_latest`, `.shrink_*`, `.default_layer`, `.layer`, `.layers`,
    `.exclude_layer`, `.exclude_layers`. All lazy — no RPC until a terminal."""
    with _make_graph_with_edge() as rg:
        # Add a second edge event on the same pair at t=8.
        rg.add_edge(8, "ben", "hamza")
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
        assert (
            e.window(0, 5).earliest_time
            == rg.window(0, 5).edge("ben", "hamza").earliest_time
        )


def test_edge_shrink_builders():
    """`.shrink_window`, `.shrink_start`, `.shrink_end` narrow an existing window."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")
        wide = rg.edge("ben", "hamza").window(0, 100)
        assert wide.earliest_time == 3
        assert wide.latest_time == 8

        assert wide.shrink_window(0, 5).latest_time == 3
        # shrink_start cuts t=3, keeps t=8.
        assert wide.shrink_start(5).earliest_time == 8
        # shrink_end keeps t=3, cuts t=8.
        assert wide.shrink_end(5).latest_time == 3


def test_edges_view_chain_propagates_through_collection_list():
    """Regression: materialized edges must carry the parent view forward, so
    view-dependent terminals give the right answer under the same view chain
    that produced the collection."""
    with _make_graph_with_edge() as rg:
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


def test_nodes_sorted_by_id():
    """`nodes.sorted([NodeSortBy.by_id()])` returns a nodes collection in
    id order — verified by `.id`. `reverse=True` flips it."""
    with _make_graph_with_edge() as rg:
        asc = rg.nodes.sorted([NodeSortBy.by_id()]).id
        assert asc == sorted(asc), f"expected ascending ids, got {asc}"

        desc = rg.nodes.sorted([NodeSortBy.by_id(reverse=True)]).id
        assert desc == sorted(
            desc, reverse=True
        ), f"expected descending ids, got {desc}"
        # Same members, both orderings.
        assert set(asc) == set(desc) == {"ben", "hamza"}


def test_nodes_sorted_by_property_and_time():
    """Sort by a temporal property and by time. Multi-key lexicographic
    sort — tiebreak on the second key when the first ties."""
    with _remote_graph("g") as rg:
        # Three nodes with distinct scores; ben earlier than hamza & zara.
        rg.add_node(1, "ben", properties={"score": 3.0})
        rg.add_node(2, "hamza", properties={"score": 1.0})
        rg.add_node(3, "zara", properties={"score": 2.0})

        by_score = rg.nodes.sorted([NodeSortBy.by_property("score")]).id
        assert by_score == [
            "hamza",
            "zara",
            "ben",
        ], f"expected ascending by score: hamza(1), zara(2), ben(3); got {by_score}"

        by_score_desc = rg.nodes.sorted(
            [NodeSortBy.by_property("score", reverse=True)]
        ).id
        assert by_score_desc == ["ben", "zara", "hamza"]

        by_earliest = rg.nodes.sorted([NodeSortBy.by_time(SortByTime.EARLIEST)]).id
        assert by_earliest == ["ben", "hamza", "zara"]

        by_latest_desc = rg.nodes.sorted(
            [NodeSortBy.by_time(SortByTime.LATEST, reverse=True)]
        ).id
        assert by_latest_desc == ["zara", "hamza", "ben"]


def test_nodes_sorted_is_lazy_and_composable():
    """`.sorted()` doesn't fire an RPC on its own; it returns a `RemoteNodes`
    that composes with downstream terminals like `.count()` and `.collect()`."""
    with _make_graph_with_edge() as rg:
        sorted_nodes = rg.nodes.sorted([NodeSortBy.by_id()])
        # Terminal still works — count == 2.
        assert sorted_nodes.count() == 2
        # `.collect()` returns full node handles in sorted order.
        materialized = sorted_nodes.collect()
        assert [n.name for n in materialized] == sorted(n.name for n in materialized)


def test_edges_sorted_by_src_dst():
    """Sort edges by src then dst — lexicographic multi-key."""
    with _remote_graph("g") as rg:
        rg.add_edge(1, "b", "c")
        rg.add_edge(2, "a", "c")
        rg.add_edge(3, "a", "b")

        sorted_edges = rg.edges.sorted(
            [EdgeSortBy.by_src(), EdgeSortBy.by_dst()]
        ).collect()
        pairs = [(e.src.name, e.dst.name) for e in sorted_edges]
        assert pairs == [
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
        ], f"expected [(a,b),(a,c),(b,c)] by (src, dst), got {pairs}"


def test_edges_sorted_by_time_and_property():
    """Sort edges by earliest observed time; also by an edge property."""
    with _remote_graph("g") as rg:
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


def test_edges_sorted_composes_with_view_chain():
    """`.sorted()` composes with a windowed view — sort applies only to
    edges visible in the window."""
    with _remote_graph("g") as rg:
        rg.add_edge(1, "a", "b")
        rg.add_edge(5, "a", "c")
        rg.add_edge(20, "b", "c")

        windowed_sorted = (
            rg.window(0, 10)
            .edges.sorted([EdgeSortBy.by_time(SortByTime.EARLIEST)])
            .collect()
        )
        pairs = [(e.src.name, e.dst.name) for e in windowed_sorted]
        # Only the first two edges are in [0, 10). Sorted by earliest time.
        assert pairs == [("a", "b"), ("a", "c")]


@contextlib.contextmanager
def _make_shared_neighbours_graph():
    """Two hub nodes (a, d) that share neighbours (b, c) plus a
    non-shared neighbour on each side (e touches only a; f touches only d).
    Shared: {b, c}. Non-shared: e (only a), f (only d)."""
    with _remote_graph("g") as rg:
        rg.add_edge(1, "a", "b")
        rg.add_edge(2, "a", "c")
        rg.add_edge(3, "a", "e")  # a only
        rg.add_edge(4, "d", "b")
        rg.add_edge(5, "d", "c")
        rg.add_edge(6, "d", "f")  # d only
        yield rg


def test_shared_neighbours_intersection():
    """`shared_neighbours` returns the intersection of neighbours across
    the input ids."""
    with _make_shared_neighbours_graph() as rg:
        shared = rg.shared_neighbours(["a", "d"])
        names = sorted(n.name for n in shared)
        assert names == ["b", "c"], f"expected [b, c], got {names}"


def test_shared_neighbours_single_node():
    """One input id returns all its neighbours (intersection of one set)."""
    with _make_shared_neighbours_graph() as rg:
        shared = rg.shared_neighbours(["a"])
        names = sorted(n.name for n in shared)
        assert names == ["b", "c", "e"]


def test_shared_neighbours_empty_and_missing():
    """Empty input list → []. Missing ids are silently dropped server-side;
    the intersection is taken over the ids that do exist. All-missing → []."""
    with _make_shared_neighbours_graph() as rg:
        # Empty input.
        assert rg.shared_neighbours([]) == []

        # `z` doesn't exist and is dropped — result is `a`'s neighbours.
        with_missing = rg.shared_neighbours(["a", "z"])
        names = sorted(n.name for n in with_missing)
        assert names == ["b", "c", "e"]

        # All ids missing → nothing to intersect → [].
        assert rg.shared_neighbours(["x", "y", "z"]) == []


def test_shared_neighbours_returns_usable_handles():
    """Returned RemoteNode handles carry the current view chain — terminals
    like `.degree()` and `.properties.get(...)` work against them."""
    with _make_shared_neighbours_graph() as rg:
        shared = rg.shared_neighbours(["a", "d"])
        for n in shared:
            # Each shared neighbour has degree 2 (connected to both a and d).
            assert n.degree() == 2


def test_neighbours_returns_remote_path_from_node():
    """`.neighbours` / `.in_neighbours` / `.out_neighbours` return the new
    `RemotePathFromNode` type (subset of `RemoteNodes` — no `.sorted` or
    `.default_layer`)."""
    from raphtory.graphql import RemotePathFromNode

    with _make_graph_with_edge() as rg:
        ben = rg.node("ben")
        # All three navigation accessors return the same type.
        assert isinstance(ben.neighbours, RemotePathFromNode)
        assert isinstance(ben.in_neighbours, RemotePathFromNode)
        assert isinstance(ben.out_neighbours, RemotePathFromNode)


def test_remote_path_from_node_terminals():
    """Terminals shared with `RemoteNodes` — `ids`, `count`, `list`, and
    native iteration — all work on the new type."""
    with _make_graph_with_edge() as rg:
        ben = rg.node("ben")
        assert ben.out_neighbours.id == ["hamza"]
        assert ben.out_neighbours.count() == 1
        materialized = ben.out_neighbours.collect()
        assert [n.name for n in materialized] == ["hamza"]
        assert [n.name for n in ben.out_neighbours] == ["hamza"]


def test_remote_path_from_node_view_chain_composes():
    """View-chain builders on `RemotePathFromNode` compose lazily. Terminals
    that inspect membership (`ids`, `list`) reflect the narrowed view."""
    with _make_graph_with_edge() as rg:
        # Add extra edges so the path has multiple members at different times.
        rg.add_edge(8, "ben", "hamza")

        # `.window()` on the path narrows the view — verified via terminals
        # that walk the collection (ids/list).
        narrowed = rg.node("ben").out_neighbours.window(0, 5)
        assert narrowed.id == ["hamza"]

        # Verify chaining preserves the type and lazy semantics.
        chained = rg.node("ben").out_neighbours.window(0, 100).layer("_default")
        assert chained.id == ["hamza"]


def test_remote_path_from_node_type_filter():
    """`.type_filter(...)` narrows membership; return type is still
    `RemotePathFromNode`."""
    from raphtory.graphql import RemotePathFromNode

    with _make_graph_with_edge() as rg:
        rg.node("hamza").set_node_type("bot")
        filtered = rg.node("ben").out_neighbours.type_filter(["bot"])
        assert isinstance(filtered, RemotePathFromNode)
        assert filtered.id == ["hamza"]

        # Filter to a non-matching type — result should be empty.
        assert rg.node("ben").out_neighbours.type_filter(["human"]).id == []


def test_remote_path_from_node_lacks_sorted():
    """`.sorted` is not exposed on `RemotePathFromNode` — matching local
    `PathFromNode`, which has no `sorted`. `.default_layer`, by contrast, IS
    exposed (local `PathFromNode` has it as a method)."""
    from raphtory import PathFromNode

    with _make_graph_with_edge() as rg:
        neighbours = rg.node("ben").out_neighbours
        assert not hasattr(
            neighbours, "sorted"
        ), "sorted must not be available on RemotePathFromNode"
        assert not hasattr(PathFromNode, "sorted")
        # default_layer is part of the local surface, so the remote exposes it
        # AND it must actually round-trip to the server (not a phantom field).
        assert hasattr(neighbours, "default_layer")
        assert hasattr(PathFromNode, "default_layer")
        assert sorted(n.name for n in neighbours.default_layer()) == sorted(
            n.name for n in neighbours
        )


def test_shared_neighbours_composes_with_view_chain():
    """`.shared_neighbours()` runs against the current view chain — the
    intersection uses the neighbours visible under that view."""
    with _make_shared_neighbours_graph() as rg:
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


@contextlib.contextmanager
def _make_filter_graph():
    """Graph with 4 nodes, distinct properties, for filter tests."""
    with _remote_graph("g") as rg:
        # Names and a numeric "score" property for filtering.
        rg.add_node(1, "ben", properties={"score": 10.0})
        rg.add_node(2, "hamza", properties={"score": 5.0})
        rg.add_node(3, "alice", properties={"score": 20.0})
        rg.add_node(4, "bob", properties={"score": 15.0})
        yield rg


def test_select_nodes_by_name_eq():
    """`Node.name() == "ben"` narrows to the single matching node."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        narrowed = rg.nodes.select(Node.name() == "ben").collect()
        assert [n.name for n in narrowed] == ["ben"]


def test_select_nodes_by_name_contains():
    """`Node.name().contains("b")` matches ben and bob."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        narrowed = rg.nodes.select(Node.name().contains("b")).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["ben", "bob"]


def test_select_nodes_by_property_gt():
    """`Node.property("score") > 12.0` narrows by numeric property."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        narrowed = rg.nodes.select(Node.property("score") > 12.0).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["alice", "bob"]


def test_select_nodes_and_combinator():
    """`(name contains "b") & (score > 12)` — only bob."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        combined = (Node.name().contains("b")) & (Node.property("score") > 12.0)
        narrowed = rg.nodes.select(combined).collect()
        assert [n.name for n in narrowed] == ["bob"]


def test_select_nodes_or_combinator():
    """`(name == "ben") | (score < 6)` — ben and hamza."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        combined = (Node.name() == "ben") | (Node.property("score") < 6.0)
        narrowed = rg.nodes.select(combined).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["ben", "hamza"]


def test_select_nodes_not_combinator():
    """`~(name == "ben")` — everyone but ben."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        narrowed = rg.nodes.select(~(Node.name() == "ben")).collect()
        names = sorted(n.name for n in narrowed)
        assert names == ["alice", "bob", "hamza"]


def test_select_nodes_returns_lazy_handle():
    """`.select()` returns a `RemoteNodes` — terminals (`.count()`,
    `.id`, `.collect()`) all work on it."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        narrowed = rg.nodes.select(Node.property("score") >= 10.0)
        assert narrowed.count() == 3
        assert sorted(narrowed.id) == ["alice", "ben", "bob"]


def test_select_nodes_composes_with_view_chain():
    """`.select()` chains with view ops (`.window()`) — both narrow the
    resulting collection."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # Window [0, 3) sees only ben (t=1) and hamza (t=2). Then filter by
        # score > 6 leaves just ben (score=10).
        narrowed = rg.window(0, 3).nodes.select(Node.property("score") > 6.0).collect()
        assert [n.name for n in narrowed] == ["ben"]


def test_select_nodes_can_chain():
    """Chained `.select()` calls compose — server applies each in turn."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # First select narrows to names containing "b"; second narrows to
        # score > 12 — only bob remains.
        narrowed = (
            rg.nodes.select(Node.name().contains("b"))
            .select(Node.property("score") > 12.0)
            .collect()
        )
        assert [n.name for n in narrowed] == ["bob"]


def test_filter_nodes_preserves_membership():
    """A *property* `.filter()` on `RemoteNodes` does NOT narrow the current
    collection — the returned collection retains all original members; the
    predicate is retained for downstream traversals. Contrast with `.select()`,
    which narrows membership at this step, and with a node-id filter (e.g.
    `Node.name() == ...`), which the engine applies as a graph view and so does
    narrow — matching local raphtory."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # score > 8.0 excludes hamza (5.0), but `.filter()` keeps every member —
        # the predicate is deferred to traversals, not applied to membership.
        all_ids = sorted(rg.nodes.filter(Node.property("score") > 8.0).id)
        assert all_ids == ["alice", "ben", "bob", "hamza"]


def test_filter_nodes_narrows_on_node_id():
    """#2690: a node-id filter (name/id) is applied as a graph view, so it DOES
    narrow collection membership — unlike the property filter above. Pins the
    new behavior (and its parity with local) so a regression is visible."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        assert sorted(rg.nodes.filter(Node.name() == "ben").id) == ["ben"]


def test_temporal_multi_op_filter_preserves_op_order_e2e():
    """End-to-end guard that a multi-op temporal filter keeps its op-order
    through the wire — the client serializes it via `apply_ops_to_condition`
    (filtering.rs), so an inversion there would corrupt the query.

    On a list-valued temporal property, `.first().sum()` is shape-valid: First
    picks the first snapshot's list, Sum reduces it to a scalar. The inversion
    `.sum().first()` reduces a sequence-of-lists (→ None) and can never match,
    so any op-order flip in the wire turns `["n"]` into `[]`. Uses the narrowing
    `graph.filter()` path (not sticky `nodes.filter`) with a distractor node,
    and pins the remote result against a local twin.
    """
    from raphtory import Graph
    from raphtory.filter import Node

    def build(g):
        # n: first snapshot [1, 2] (sum 3); d: first snapshot [8, 9] (sum 17).
        g.add_node(0, "n", properties={"x": [1, 2]})
        g.add_node(1, "n", properties={"x": [3, 4]})
        g.add_node(0, "d", properties={"x": [8, 9]})
        g.add_node(1, "d", properties={"x": [10, 11]})

    first_sum_3 = Node.property("x").temporal().first().sum() == 3
    first_sum_17 = Node.property("x").temporal().first().sum() == 17

    local = Graph()
    build(local)
    assert sorted(local.filter(first_sum_3).nodes.id) == ["n"]

    with _remote_graph("g") as rg:
        build(rg)
        # Remote must agree with the local twin. An op-order inversion in the
        # wire would sum-then-first (seq-of-lists → None) and return [].
        assert sorted(rg.filter(first_sum_3).nodes.id) == ["n"]
        assert sorted(rg.filter(first_sum_17).nodes.id) == ["d"]


@contextlib.contextmanager
def _make_edge_filter_graph():
    """Graph with 4 edges carrying a numeric "weight" property, for edge
    filter tests."""
    with _remote_graph("g") as rg:
        rg.add_edge(1, "ben", "hamza", properties={"weight": 10.0})
        rg.add_edge(2, "ben", "alice", properties={"weight": 5.0})
        rg.add_edge(3, "alice", "bob", properties={"weight": 20.0})
        rg.add_edge(4, "bob", "hamza", properties={"weight": 15.0})
        yield rg


def _edge_pairs(edges):
    """(src, dst) name pairs for a list of RemoteEdge, sorted."""
    return sorted((e.src.name, e.dst.name) for e in edges)


def test_select_edges_by_property_gt():
    """`Edge.property("weight") > 12.0` narrows by numeric property."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        narrowed = rg.edges.select(Edge.property("weight") > 12.0).collect()
        assert _edge_pairs(narrowed) == [("alice", "bob"), ("bob", "hamza")]


def test_select_edges_by_src_name():
    """`Edge.src().name() == "ben"` narrows to edges out of ben."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        narrowed = rg.edges.select(Edge.src().name() == "ben").collect()
        assert _edge_pairs(narrowed) == [("ben", "alice"), ("ben", "hamza")]


def test_select_edges_by_dst_name():
    """`Edge.dst().name() == "hamza"` narrows to edges into hamza."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        narrowed = rg.edges.select(Edge.dst().name() == "hamza").collect()
        assert _edge_pairs(narrowed) == [("ben", "hamza"), ("bob", "hamza")]


def test_select_edges_and_combinator():
    """`(src == "ben") & (weight > 6)` — only ben-hamza (ben-alice has
    weight 5)."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        combined = (Edge.src().name() == "ben") & (Edge.property("weight") > 6.0)
        narrowed = rg.edges.select(combined).collect()
        assert _edge_pairs(narrowed) == [("ben", "hamza")]


def test_select_edges_or_combinator():
    """`(weight > 18) | (src == "ben")` — alice-bob, ben-hamza, ben-alice."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        combined = (Edge.property("weight") > 18.0) | (Edge.src().name() == "ben")
        narrowed = rg.edges.select(combined).collect()
        assert _edge_pairs(narrowed) == [
            ("alice", "bob"),
            ("ben", "alice"),
            ("ben", "hamza"),
        ]


def test_select_edges_not_combinator():
    """`~(dst == "hamza")` — every edge not into hamza."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        narrowed = rg.edges.select(~(Edge.dst().name() == "hamza")).collect()
        assert _edge_pairs(narrowed) == [("alice", "bob"), ("ben", "alice")]


def test_select_edges_returns_lazy_handle():
    """`.select()` returns a `RemoteEdges` — terminals (`.count()`, `.collect()`)
    all work on it."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        narrowed = rg.edges.select(Edge.property("weight") >= 10.0)
        assert narrowed.count() == 3
        assert _edge_pairs(narrowed.collect()) == [
            ("alice", "bob"),
            ("ben", "hamza"),
            ("bob", "hamza"),
        ]


def test_select_edges_composes_with_view_chain():
    """`.select()` chains with view ops (`.window()`) — both narrow the
    resulting collection."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        # Window [0, 3) sees only ben-hamza (t=1) and ben-alice (t=2). Then
        # filter by weight > 6 leaves just ben-hamza (weight=10).
        narrowed = rg.window(0, 3).edges.select(Edge.property("weight") > 6.0).collect()
        assert _edge_pairs(narrowed) == [("ben", "hamza")]


def test_select_edges_can_chain():
    """Chained `.select()` calls compose — server applies each in turn."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        # First select narrows to edges out of ben; second narrows to
        # weight > 6 — only ben-hamza remains.
        narrowed = (
            rg.edges.select(Edge.src().name() == "ben")
            .select(Edge.property("weight") > 6.0)
            .collect()
        )
        assert _edge_pairs(narrowed) == [("ben", "hamza")]


def test_filter_edges_preserves_membership():
    """`.filter()` on `RemoteEdges` does NOT narrow the current collection —
    the returned collection retains all original members. The filter is
    retained for downstream traversals. Contrast with `.select()`, which
    narrows membership at this step (tested above)."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        # `.filter()` preserves current collection membership.
        kept = rg.edges.filter(Edge.src().name() == "ben").collect()
        assert _edge_pairs(kept) == [
            ("alice", "bob"),
            ("ben", "alice"),
            ("ben", "hamza"),
            ("bob", "hamza"),
        ]


# --- unified .filter() on Graph / Node / PathFromNode -------------


@contextlib.contextmanager
def _make_node_filter_graph():
    """Hub node 'ben' with three out-neighbours carrying a 'score' property,
    for Node.filter / PathFromNode.filter/select tests."""
    with _remote_graph("g") as rg:
        rg.add_node(1, "ben", properties={"score": 100.0})
        rg.add_node(1, "hamza", properties={"score": 5.0})
        rg.add_node(1, "alice", properties={"score": 20.0})
        rg.add_node(1, "bob", properties={"score": 15.0})
        rg.add_edge(1, "ben", "hamza")
        rg.add_edge(1, "ben", "alice")
        rg.add_edge(1, "ben", "bob")
        yield rg


def test_graph_filter_dispatches_node_filter():
    """`RemoteGraph.filter(<node filter>)` routes to the server `filterNodes`
    field — matching the local unified `Graph.filter`. Keeps matching nodes."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # score > 12: alice (20) and bob (15); ben (10) and hamza (5) drop.
        filtered = rg.filter(Node.property("score") > 12.0)
        assert sorted(filtered.nodes.id) == ["alice", "bob"]


def test_graph_filter_dispatches_edge_filter():
    """`RemoteGraph.filter(<edge filter>)` routes to the server `filterEdges`
    field. Keeps matching edges; nodes remain even if all their edges drop."""
    from raphtory.filter import Edge

    with _make_edge_filter_graph() as rg:
        # weight > 12: alice-bob (20) and bob-hamza (15).
        filtered = rg.filter(Edge.property("weight") > 12.0)
        assert _edge_pairs(filtered.edges.collect()) == [
            ("alice", "bob"),
            ("bob", "hamza"),
        ]


def test_datetime_property_filter_is_accepted():
    """Filter values that are datetimes must render with the schema's field
    casing (`dtime`/`ndtime`). Before the fix the read path emitted camelCase
    (`dTime`/`nDTime`) and the server rejected every datetime filter value."""
    from datetime import datetime, timezone
    from raphtory.filter import Node

    with _make_graph_with_edge() as rg:
        aware = datetime(2020, 1, 1, tzinfo=timezone.utc)  # -> {dtime: ...}
        naive = datetime(2021, 6, 1)  # -> {ndtime: ...}
        rg.node("ben").add_updates(5, properties={"created": aware, "seen": naive})
        # Both must be accepted by the server (no GraphQL error) and select ben.
        assert sorted(rg.filter(Node.property("created") == aware).nodes.id) == ["ben"]
        assert sorted(rg.filter(Node.property("seen") == naive).nodes.id) == ["ben"]


def test_graph_filter_composes_with_view_chain():
    """`.filter()` composes with a graph-level view op."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # All four nodes are at t=1..4; window [0,3) keeps ben (t=1) and
        # hamza (t=2). Filter score > 6 then leaves only ben (10).
        filtered = rg.window(0, 3).filter(Node.property("score") > 6.0)
        assert sorted(filtered.nodes.id) == ["ben"]


def test_node_filter_matches():
    """`RemoteNode.filter(<node filter>)` mirrors local `Node.filter` — a
    terminal on a node that matches the filter still resolves."""
    from raphtory.filter import Node

    with _make_filter_graph() as rg:
        # ben (score=10) matches score > 6; the name terminal still resolves.
        assert rg.node("ben").filter(Node.property("score") > 6.0).name == "ben"


def test_node_filter_accepts_edge_filter():
    """An edge filter on a node view is valid (matching local semantics): the
    node stays addressable and the filter propagates to its edge traversals."""
    from raphtory.filter import Edge

    with _remote_graph("g") as rg:
        rg.add_edge(2, "ben", "hamza", properties={"weight": 2.0})
        rg.add_edge(3, "ben", "alice", properties={"weight": 0.5})
        filtered = rg.node("ben").filter(Edge.property("weight") > 1.0)
        assert filtered.degree() == 1
        assert rg.node("ben").degree() == 2


def test_path_from_node_select_narrows():
    """`.select()` on a neighbours path narrows membership at this hop."""
    from raphtory.filter import Node

    with _make_node_filter_graph() as rg:
        # ben's out-neighbours: hamza (5), alice (20), bob (15).
        # select score > 12 → alice, bob.
        narrowed = rg.node("ben").out_neighbours.select(Node.property("score") > 12.0)
        assert sorted(narrowed.id) == ["alice", "bob"]


def test_path_from_node_filter_preserves_membership():
    """`.filter()` on a neighbours path preserves membership (propagates to
    downstream traversals instead of narrowing here)."""
    from raphtory.filter import Node

    with _make_node_filter_graph() as rg:
        kept = rg.node("ben").out_neighbours.filter(Node.property("score") > 12.0)
        assert sorted(kept.id) == ["alice", "bob", "hamza"]


# --- collection ergonomics: len()/bool() + dict-protocol ---------------------


def test_collection_len_and_bool():
    """`len()` / `bool()` on remote collections map to `.count()`."""
    with _make_filter_graph() as rg:  # 4 nodes, no edges
        assert len(rg.nodes) == 4
        assert bool(rg.nodes) is True
        # No edges in this graph.
        assert len(rg.edges) == 0
        assert bool(rg.edges) is False


def test_path_from_node_len():
    """`len()` on a neighbours path (`RemotePathFromNode`)."""
    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        assert len(rg.node("ben").out_neighbours) == 3
        assert bool(rg.node("ben").out_neighbours) is True


def test_nodes_out_neighbours_path_from_graph_count():
    """`RemoteNodes.out_neighbours` returns a `RemotePathFromGraph` whose
    `count()` == the number of source nodes. The nested `ids()` / `collect()`
    terminals are exercised by `test_nodes_out_neighbours_path_from_graph`."""
    from raphtory.graphql import RemotePathFromGraph

    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        path = rg.nodes.out_neighbours
        assert isinstance(path, RemotePathFromGraph)
        # 4 source nodes → 4 source paths.
        assert path.count() == 4


# --- multi-hop traversal on the two path collection types --------------------


def test_path_from_node_multi_hop_flat():
    """`RemoteNode.out_neighbours.out_neighbours` chains and stays a flat
    `RemotePathFromNode`; `.collect()` returns a flat `list[RemoteNode]`."""
    from raphtory.graphql import RemoteNode, RemotePathFromNode

    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        two_hop = rg.node("ben").out_neighbours.out_neighbours
        assert isinstance(two_hop, RemotePathFromNode)
        # hamza/alice/bob have no out-edges → the 2-hop is flat and empty.
        collected = two_hop.collect()
        assert isinstance(collected, list)
        assert collected == []
        # A non-empty 2-hop stays flat: each of hamza/alice/bob neighbours
        # (both directions) back to ben, flattened into a single list.
        back = rg.node("ben").out_neighbours.neighbours.collect()
        assert isinstance(back, list)
        assert all(isinstance(n, RemoteNode) for n in back)
        assert sorted(n.name for n in back) == ["ben", "ben", "ben"]


def test_path_from_node_edges_flat():
    """`RemoteNode.out_neighbours.out_edges` returns a flat `RemoteEdges`."""
    from raphtory.graphql import RemoteEdges

    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        out_edges = rg.node("ben").out_neighbours.out_edges
        assert isinstance(out_edges, RemoteEdges)
        # hamza/alice/bob have no out-edges.
        assert out_edges.count() == 0
        # Their incoming edges are the three ben->X edges, flattened.
        in_edges = rg.node("ben").out_neighbours.in_edges
        assert isinstance(in_edges, RemoteEdges)
        assert _edge_pairs(in_edges.collect()) == [
            ("ben", "alice"),
            ("ben", "bob"),
            ("ben", "hamza"),
        ]


def test_path_from_graph_multi_hop_nested():
    """`RemoteNodes.out_neighbours.out_neighbours` chains and stays a nested
    `RemotePathFromGraph`; `.collect()` → `list[list[RemoteNode]]`."""
    from raphtory.graphql import RemoteNode, RemotePathFromGraph

    with _make_node_filter_graph() as rg:  # 4 source nodes
        two_hop = rg.nodes.out_neighbours.out_neighbours
        assert isinstance(two_hop, RemotePathFromGraph)
        collected = two_hop.collect()
        # Nested: one inner list per source node (4 sources).
        assert isinstance(collected, list)
        assert len(collected) == 4
        assert all(isinstance(row, list) for row in collected)
        # Only ben has out-neighbours, and those have no out-neighbours → all
        # inner lists are empty, but the nesting (per-source rows) is preserved.
        assert all(row == [] for row in collected)
        # A non-empty nested 2-hop stays nested list[list[RemoteNode]].
        back = rg.nodes.out_neighbours.neighbours.collect()
        assert all(isinstance(row, list) for row in back)
        assert all(isinstance(n, RemoteNode) for row in back for n in row)


def test_path_from_graph_edges_nested():
    """`RemoteNodes.out_neighbours.out_edges` returns a nested
    `RemoteNestedEdges`; `.collect()` → `list[list[RemoteEdge]]`."""
    from raphtory.graphql import RemoteNestedEdges

    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        nested = rg.nodes.out_neighbours.out_edges
        assert isinstance(nested, RemoteNestedEdges)
        collected = nested.collect()
        # Nested: list of per-source lists of RemoteEdge.
        assert isinstance(collected, list)
        assert all(isinstance(row, list) for row in collected)
        # The incoming edges of every node's out-neighbours are nested too.
        nested_in = rg.nodes.out_neighbours.in_edges
        assert isinstance(nested_in, RemoteNestedEdges)
        in_collected = nested_in.collect()
        assert isinstance(in_collected, list)
        assert all(isinstance(row, list) for row in in_collected)
        # ben's out-neighbours (hamza/alice/bob) each have one incoming ben->X
        # edge; flattening the nested rows recovers all three.
        flat_pairs = _edge_pairs(e for row in in_collected for e in row)
        assert flat_pairs == [
            ("ben", "alice"),
            ("ben", "bob"),
            ("ben", "hamza"),
        ]


def test_nodes_out_neighbours_path_from_graph():
    """`RemoteNodes.out_neighbours` returns a nested `RemotePathFromGraph`.

    Graph: ben -> hamza, alice, bob (4 source nodes total). `collect()` and
    `ids()` are nested (one inner list per source node); `count()` is the
    number of source paths (== number of source nodes).
    """
    from raphtory.graphql import RemotePathFromGraph

    with _make_node_filter_graph() as rg:
        path = rg.nodes.out_neighbours
        assert isinstance(path, RemotePathFromGraph)

        collected = path.collect()
        # Nested: list of per-source lists of RemoteNode.
        assert isinstance(collected, list)
        assert all(isinstance(row, list) for row in collected)

        ids = path.id
        # Nested: list of per-source lists of str.
        assert isinstance(ids, list)
        assert all(isinstance(row, list) for row in ids)
        assert all(isinstance(x, str) for row in ids for x in row)

        # One source path per source node.
        assert path.count() == 4
        assert len(ids) == 4
        assert len(collected) == 4

        # ben's out-neighbours are hamza, alice, bob; the other three source
        # nodes have none. Exactly one inner list holds all three.
        assert sorted(next(row for row in ids if len(row) == 3)) == [
            "alice",
            "bob",
            "hamza",
        ]

        # `collect()` yields RemoteNode handles whose names match the ids.
        collected_names = [[n.name for n in row] for row in collected]
        assert sorted(next(row for row in collected_names if len(row) == 3)) == [
            "alice",
            "bob",
            "hamza",
        ]

        # Native iteration yields each per-source list.
        iterated = [list(row) for row in path]
        assert len(iterated) == 4


def test_nodes_out_edges_nested_edges():
    """`RemoteNodes.out_edges` returns a nested `RemoteNestedEdges`.

    Graph: ben -> hamza, alice, bob (4 source nodes total). `collect()` is
    nested (one inner list per source node); `count()` is the number of source
    edge collections (== number of source nodes).
    """
    from raphtory.graphql import RemoteNestedEdges

    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        nested = rg.nodes.out_edges
        assert isinstance(nested, RemoteNestedEdges)

        collected = nested.collect()
        # Nested: list of per-source lists of RemoteEdge.
        assert isinstance(collected, list)
        assert all(isinstance(row, list) for row in collected)

        # One source edge collection per source node.
        assert nested.count() == 4
        assert len(collected) == 4

        # ben's out-edges are (ben, hamza), (ben, alice), (ben, bob); the other
        # three source nodes have none. Exactly one inner list holds all three.
        ben_row = next(row for row in collected if len(row) == 3)
        assert all(e.src.name == "ben" for e in ben_row)
        assert sorted(e.dst.name for e in ben_row) == ["alice", "bob", "hamza"]

        # Native iteration yields each per-source list.
        iterated = [list(row) for row in nested]
        assert len(iterated) == 4


def test_metadata_dict_protocol():
    """`RemoteMetadata` is dict-like: `md[k]`, `k in md`, `len(md)`,
    `for k in md`, `md.as_dict()`; `md[missing]` raises `KeyError`."""
    import pytest

    with _make_graph_with_edge() as rg:
        rg.node("ben").add_metadata({"role": "admin", "level": 3, "active": True})
        md = rg.node("ben").metadata
        assert md["role"] == "admin"  # __getitem__ → raw value
        assert md["level"] == 3
        assert md["active"] is True
        assert "role" in md  # __contains__
        assert "nonexistent" not in md
        assert len(md) == 3  # __len__
        assert sorted(md) == ["active", "level", "role"]  # __iter__ over keys
        assert md.as_dict() == {"role": "admin", "level": 3, "active": True}
        with pytest.raises(KeyError):  # strict, unlike .get()
            md["nonexistent"]
        assert md.get("nonexistent") is None


def test_properties_dict_protocol():
    """`RemoteProperties` is dict-like too; values are the latest temporal
    value under the current view."""
    import pytest

    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 2.5})
        props = rg.node("ben").properties
        assert props["score"] == 2.5
        assert "score" in props
        assert "nonexistent" not in props
        assert len(props) == 1
        assert list(props) == ["score"]
        assert props.as_dict() == {"score": 2.5}
        with pytest.raises(KeyError):
            props["nonexistent"]


def test_map_property_preserves_key_order():
    """A map-valued property round-trips through the server with its key
    insertion order intact — same order a local graph would report."""
    from raphtory import Graph

    cfg = {"zeta": 1, "alpha": 2, "mid": 3}

    with _remote_graph() as rg:
        rg.add_node(1, "n", properties={"cfg": cfg})
        remote_cfg = rg.node("n").properties["cfg"]
        assert remote_cfg == cfg
        assert list(remote_cfg) == ["zeta", "alpha", "mid"]

    g = Graph()
    g.add_node(1, "n", properties={"cfg": cfg})
    assert list(g.node("n").properties["cfg"]) == ["zeta", "alpha", "mid"]


def test_non_finite_floats_round_trip():
    """NaN and ±Infinity survive a remote write → read round-trip — JSON has
    no number form for them, so they ride tagged variants on the way in and
    string sentinels (decoded via dtype) on the way out."""
    import math

    with _remote_graph() as rg:
        rg.add_node(1, "n", properties={"nan": float("nan"), "inf": float("inf")})
        props = rg.node("n").properties
        assert math.isnan(props["nan"])
        assert props["inf"] == float("inf")


def test_property_dtype_fidelity_remote():
    """Stored values decode to their exact dtype remotely, not the widest
    JSON-shaped variant — matching what a local graph reports."""
    from raphtory import Graph, Prop, PropType

    with _remote_graph() as rg:
        rg.add_node(1, "n", properties={"small": Prop.u8(7), "single": Prop.f32(1.5)})
        props = rg.node("n").properties
        assert props.get_dtype_of("small") == PropType.u8()
        remote_small = props["small"]
        assert remote_small == 7

    g = Graph()
    g.add_node(1, "n", properties={"small": Prop.u8(7)})
    assert g.node("n").properties["small"] == 7


def test_collection_getitem_is_select():
    """`nodes[filter]` / `edges[filter]` are sugar for `.select(filter)` —
    matching the local API, where `__getitem__` takes a FilterExpr."""
    from raphtory.filter import Edge, Node

    with _make_filter_graph() as rg:  # 4 nodes with a score property
        # nodes[<node filter>] == nodes.select(<node filter>)
        assert sorted(rg.nodes[Node.property("score") > 12.0].id) == ["alice", "bob"]

    with _make_edge_filter_graph() as rg:
        # edges[<edge filter>] == edges.select(<edge filter>)
        got = _edge_pairs(rg.edges[Edge.property("weight") > 12.0].collect())
        assert got == [("alice", "bob"), ("bob", "hamza")]


def test_node_edge_getitem_property():
    """`node[key]` / `edge[key]` return the property value. `node[missing]`
    raises `KeyError`; `edge[missing]` returns `None` (matches local)."""
    import pytest

    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 2.5})
        rg.add_edge(6, "ben", "hamza", properties={"weight": 9.0})
        assert rg.node("ben")["score"] == 2.5
        with pytest.raises(KeyError):
            rg.node("ben")["nonexistent"]

        assert rg.edge("ben", "hamza")["weight"] == 9.0
        assert rg.edge("ben", "hamza")["nonexistent"] is None


def test_event_time_fields():
    """`EventTime` exposes `.t`, `.event_id`, `.dt` (a real
    `datetime`), and `.as_tuple` — mirroring the local `EventTime`."""
    import datetime as _dt

    with _make_graph_with_edge() as rg:  # ben added at t=1
        et = rg.node("ben").earliest_time  # property → RemoteEventTime
        assert et.t == 1
        assert et == 1  # richcmp against int (by timestamp)
        assert isinstance(et.event_id, int)
        # .dt is a real datetime (not a string), matching local EventTime.dt
        assert isinstance(et.dt, _dt.datetime)
        assert et.dt.year == 1970
        # .as_tuple == (timestamp, event_id)
        assert et.as_tuple == (et.t, et.event_id)


def test_add_properties_event_id():
    """`add_properties(..., event_id=N)` locks the secondary index — proven by
    reading it back through the graph property's event history."""
    with _make_graph_with_edge() as rg:
        rg.add_properties(100, {"score": 1.5}, event_id=7)

        tp = rg.properties.temporal.get("score")
        assert tp is not None
        at_100 = [e for e in tp.history.collect() if e.t == 100]
        assert len(at_100) == 1
        assert at_100[0].event_id == 7  # the locked index, not an auto-increment

        # Omitting event_id still works (server auto-increments).
        rg.add_properties(101, {"score": 2.5})
        assert rg.properties.get("score") == 2.5


def test_valid_layers_view_ops():
    """`valid_layers` / `exclude_valid_layer` / `exclude_valid_layers` are lazy
    view builders present on every view type. They mirror `layers` /
    `exclude_layers` but require the named layers to exist. All are polymorphic
    (return the same self-type); assert the returned type and that a terminal
    runs under the layer restriction."""
    with _make_graph_with_edge() as rg:
        # Two layers now exist: "_default" (base edge) and "knows" (added here).
        rg.add_edge(4, "ben", "hamza", layer="knows")
        # On the graph — returns RemoteGraph.
        assert type(rg.valid_layers(["_default"])).__name__ == "RemoteGraph"
        # Restrict to _default (the base edge lives there): ben has degree 1.
        assert rg.valid_layers(["_default"]).node("ben").degree() == 1
        # Exclude the "knows" layer — the _default edge is still visible.
        assert rg.exclude_valid_layer("knows").node("ben").degree() == 1
        # Exclude _default via the plural form — the "knows" edge remains.
        assert rg.exclude_valid_layers(["_default"]).node("ben").degree() == 1

        # On a RemoteNodes collection — returns RemoteNodes, terminal runs.
        nodes = rg.nodes.valid_layers(["_default"])
        assert type(nodes).__name__ == "RemoteNodes"
        assert nodes.count() == 2

        # On a RemoteEdges collection — returns RemoteEdges, terminal runs.
        edges = rg.edges.exclude_valid_layer("knows")
        assert type(edges).__name__ == "RemoteEdges"
        assert edges.count() >= 1

        # On a RemoteNode — returns RemoteNode, terminal runs.
        node = rg.node("ben").valid_layers(["_default", "knows"])
        assert type(node).__name__ == "RemoteNode"
        assert node.degree() == 1

        # On a RemoteEdge — returns RemoteEdge, terminal runs.
        edge = rg.edge("ben", "hamza").exclude_valid_layers(["knows"])
        assert type(edge).__name__ == "RemoteEdge"
        assert edge.is_self_loop() is False


def test_temporal_properties_items_and_value():
    """`RemoteTemporalProperties.items()` pairs each key with its handle;
    `RemoteTemporalProperty.value()` is an alias for `.latest()`."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        tp = rg.node("ben").properties.temporal

        # items() — list of (key, handle) pairs.
        items = tp.items()
        assert {k for k, _ in items} == {"score", "active"}
        for key, handle in items:
            assert handle.key == key

        # value() == latest() for each property.
        by_key = dict(items)
        assert by_key["score"].value() == by_key["score"].latest()
        assert by_key["score"].value() == 2.5
        assert by_key["active"].value() == by_key["active"].latest()


def test_has_layer():
    """`has_layer(name)` — method firing one RPC — on graph, node, and a
    node collection. True for present layers (`_default`, `knows`), False
    otherwise."""
    with _make_graph_with_edge() as rg:
        # Add an edge on a distinct `knows` layer.
        rg.add_edge(4, "ben", "hamza", None, "knows")
        assert rg.has_layer("_default") is True
        assert rg.has_layer("knows") is True
        assert rg.has_layer("nope") is False

        # Spot-check on a node and on a collection — same terminal, polymorphic.
        assert rg.node("ben").has_layer("knows") is True
        assert rg.node("ben").has_layer("nope") is False
        assert rg.nodes.has_layer("_default") is True
        assert rg.nodes.has_layer("nope") is False


def test_window_size():
    """`window_size` — a `@property` (getter). Returns `end - start` under a
    bounded window, `None` for an unbounded view."""
    with _make_graph_with_edge() as rg:
        # Bounded window [0, 5) → size 5.
        assert rg.window(0, 5).window_size == 5
        # Unbounded view → None.
        assert rg.window_size is None

        # Polymorphic: node and collection expose the same getter.
        assert rg.window(0, 5).node("ben").window_size == 5
        assert rg.node("ben").window_size is None
        assert rg.window(2, 9).nodes.window_size == 7


def test_combined_history():
    """`PathFromNode.combined_history()` — a method returning a single
    `RemoteHistory` merging the histories of all reachable nodes."""
    with _make_graph_with_edge() as rg:
        # ben's out-neighbours == {hamza}; the combined history equals hamza's.
        ch = rg.node("ben").out_neighbours.combined_history()
        hamza_hist = rg.node("hamza").history

        assert ch.count() >= 1
        assert ch.count() == hamza_hist.count()
        assert sorted(e.t for e in ch.collect()) == sorted(
            e.t for e in hamza_hist.collect()
        )


def test_history_reverse():
    """`RemoteHistory.reverse()` — a method returning a new history whose
    iteration order is flipped. `reverse().collect()` equals `collect_rev()`."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")
        h = rg.node("ben").history
        forward = [e.t for e in h.collect()]
        reversed_collect = [e.t for e in h.reverse().collect()]

        assert reversed_collect == [e.t for e in h.collect_rev()]
        assert reversed_collect == list(reversed(forward))


def test_temporal_property_items():
    """`RemoteTemporalProperty.items()` zips history event times with values
    element-wise (2 RPCs). `__iter__` yields the same pairs."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 1.5})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        rg.node("ben").add_updates(15, properties={"score": 3.5})
        score = rg.node("ben").properties.temporal.get("score")

        items = score.items()
        hist = score.history.collect()
        vals = score.values()

        # One pair per update, aligned with history + values.
        assert len(items) == len(vals) == len(hist)
        assert [t.t for (t, _v) in items] == [e.t for e in hist]
        assert [v for (_t, v) in items] == vals

        # __iter__ yields the same pairs.
        via_iter = [(t.t, v) for (t, v) in score]
        assert via_iter == [(e.t, v) for e, v in zip(hist, vals)]


def test_nodes_collection_degree_flat():
    """`RemoteNodes.{degree,in_degree,out_degree}()` return flat `list[int]`,
    one entry per node. Graph: ben -> hamza, alice, bob."""
    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        out_deg = rg.nodes.out_degree()
        assert isinstance(out_deg, list)
        assert all(isinstance(x, int) for x in out_deg)
        # ben has 3 out-edges; hamza/alice/bob have 0. Order may vary.
        assert sorted(out_deg) == [0, 0, 0, 3]

        in_deg = rg.nodes.in_degree()
        # ben has 0 incoming; hamza/alice/bob have 1 each.
        assert sorted(in_deg) == [0, 1, 1, 1]

        deg = rg.nodes.degree()
        # ben=3, each leaf=1.
        assert sorted(deg) == [1, 1, 1, 3]


def test_path_from_node_degree_flat():
    """`RemotePathFromNode.degree()` (from `RemoteNode.out_neighbours`) returns
    a flat `list[int]`, one entry per neighbour node."""
    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        # ben's out-neighbours are hamza, alice, bob; each has degree 1.
        deg = rg.node("ben").out_neighbours.degree()
        assert isinstance(deg, list)
        assert all(isinstance(x, int) for x in deg)
        assert sorted(deg) == [1, 1, 1]
        # Each out-neighbour has out-degree 0.
        assert sorted(rg.node("ben").out_neighbours.out_degree()) == [0, 0, 0]


def test_path_from_graph_degree_nested():
    """`RemotePathFromGraph.out_degree()` (from `RemoteNodes.out_neighbours`)
    returns a nested `list[list[int]]`, one inner list per source node."""
    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        nested = rg.nodes.out_neighbours.out_degree()
        assert isinstance(nested, list)
        assert all(isinstance(row, list) for row in nested)
        assert all(isinstance(x, int) for row in nested for x in row)
        # One inner list per source node (4 sources).
        assert len(nested) == 4
        # ben's out-neighbours are hamza/alice/bob (out-degree 0 each) → [0,0,0];
        # the other three source nodes have no out-neighbours → [].
        assert sorted(len(row) for row in nested) == [0, 0, 0, 3]
        assert sorted(x for row in nested for x in row) == [0, 0, 0]

        # degree() nested: ben's out-neighbours each have degree 1.
        nested_deg = rg.nodes.out_neighbours.degree()
        assert sorted(x for row in nested_deg for x in row) == [1, 1, 1]


def test_collection_edge_history_count():
    """`edge_history_count()` on the node collections: flat on `RemoteNodes`,
    nested on `RemotePathFromGraph`."""
    with _make_node_filter_graph() as rg:  # ben -> hamza, alice, bob
        # Each edge is a single update at t=1; ben has 3 incident edges, the
        # leaves have 1 each.
        flat = rg.nodes.edge_history_count()
        assert isinstance(flat, list)
        assert sorted(flat) == [1, 1, 1, 3]

        # Nested via PathFromGraph: ben's out-neighbours each see 1 edge update.
        nested = rg.nodes.out_neighbours.edge_history_count()
        assert all(isinstance(row, list) for row in nested)
        assert sorted(x for row in nested for x in row) == [1, 1, 1]


# ---------------------------------------------------------------------------
# Client-only drop-in parity additions: naming aliases, protocol dunders,
# and TemporalProperties.latest(). All compose existing remote terminals.
# ---------------------------------------------------------------------------


def test_event_time_t():
    """`RemoteEventTime.t` returns the timestamp, mirroring local `EventTime.t`.
    Local exposes only `.t` — there is no `.timestamp`."""
    with _make_graph_with_edge() as rg:
        et = rg.node("ben").history.earliest_time()
        assert et.t == 1
        # strict parity: the non-local name is gone.
        assert not hasattr(et, "timestamp")


def test_history_t_dt():
    """`History.t` / `History.dt` return the timestamp / datetime sub-collections,
    mirroring local `History.t` / `History.dt`. Local exposes only `.t`/`.dt` —
    there is no `.timestamps`/`.datetimes`."""
    with _make_graph_with_edge() as rg:
        h = rg.node("ben").history
        # `.t` is the int-timestamp view; `.dt` the datetime view.
        assert h.t.collect() == [e.t for e in h]
        assert len(h.dt.collect()) == len(h.t.collect())
        # strict parity: the non-local names are gone.
        assert not hasattr(h, "timestamps")
        assert not hasattr(h, "datetimes")


def test_history_sequence_dunders():
    """`RemoteHistory` sequence protocol: `len`, indexing (incl. negative),
    membership, and `reversed`."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")  # ben events now at t=1, 3, 8
        h = rg.node("ben").history
        assert len(h) == 3

        # __getitem__ returns the i-th RemoteEventTime; negative indexing works.
        assert h[0].t == 1
        assert h[2].t == 8
        assert h[-1].t == 8
        assert h[-3].t == 1

        with pytest.raises(IndexError):
            _ = h[3]
        with pytest.raises(IndexError):
            _ = h[-4]

        # __contains__ — by RemoteEventTime and by bare int timestamp.
        first = h[0]
        assert first in h
        assert 1 in h
        assert 999 not in h

        # __reversed__ — descending order.
        assert [e.t for e in reversed(h)] == [8, 3, 1]


def test_history_subcollection_dunders_and_to_list():
    """Sub-collections (`t`, `event_id`, `intervals`, `dt`) support the sequence
    protocol; the int-valued ones also expose `to_list`/`to_list_rev` aliases of
    `collect`/`collect_rev`."""
    with _make_graph_with_edge() as rg:
        rg.add_edge(8, "ben", "hamza")  # ben events at t=1, 3, 8
        ts = rg.node("ben").history.t
        assert len(ts) == 3
        assert ts[0] == 1
        assert ts[-1] == 8
        assert list(ts) == [1, 3, 8]
        assert 3 in ts
        assert 999 not in ts
        assert list(reversed(ts)) == [8, 3, 1]
        with pytest.raises(IndexError):
            _ = ts[5]

        # to_list / to_list_rev aliases (t, event_id, intervals)
        assert ts.to_list() == ts.collect() == [1, 3, 8]
        assert ts.to_list_rev() == ts.collect_rev() == [8, 3, 1]

        eids = rg.node("ben").history.event_id
        assert len(eids) == 3
        assert eids.to_list() == eids.collect()
        assert list(eids) == eids.collect()

        intervals = rg.node("ben").history.intervals
        # gaps between consecutive events: (3-1), (8-3) = [2, 5]
        assert intervals.to_list() == intervals.collect() == [2, 5]
        # to_list_rev is a pure alias of collect_rev (server's own reverse
        # semantics; don't hard-code the value here).
        assert intervals.to_list_rev() == intervals.collect_rev()
        assert len(intervals) == 2
        assert intervals[0] == 2
        assert 5 in intervals
        # __reversed__ composes collect_rev (server reverse semantics).
        assert list(reversed(intervals)) == intervals.collect_rev()

        # datetimes: sequence protocol but NO to_list (matches local).
        dts = rg.node("ben").history.dt
        assert len(dts) == 3
        assert list(dts) == dts.collect()
        assert dts[0] == dts.collect()[0]
        assert dts[0] in dts
        assert list(reversed(dts)) == dts.collect_rev()
        assert not hasattr(dts, "to_list")


def test_temporal_properties_dict_dunders_and_latest():
    """`RemoteTemporalProperties` dict protocol (`__getitem__`, `__contains__`,
    `__len__`, `__iter__`) plus `latest()` mapping key -> latest value."""
    with _make_graph_with_edge() as rg:
        rg.node("ben").add_updates(5, properties={"score": 1.5, "active": True})
        rg.node("ben").add_updates(10, properties={"score": 2.5})
        td = rg.node("ben").properties.temporal

        # __getitem__ — returns a RemoteTemporalProperty; KeyError if absent.
        score = td["score"]
        assert score.key == "score"
        with pytest.raises(KeyError):
            _ = td["nonexistent"]

        # __contains__
        assert "score" in td
        assert "nonexistent" not in td

        # __len__
        assert len(td) == 2

        # __iter__ yields keys
        assert sorted(list(td)) == ["active", "score"]

        # latest() — dict of key -> latest value
        latest = td.latest()
        assert isinstance(latest, dict)
        assert latest == {"score": 2.5, "active": True}


# ============ collection-level columnar accessors ============
#
# Verifies the remote columnar accessors (`name`, `node_type`, `id` on
# node-side collections; `id`, `layer_names`, `layer_name`, `earliest_time`,
# `latest_time`, `time` on edge-side collections) match what the local
# `raphtory` API produces for the same graph.


@contextlib.contextmanager
def _make_columnar_graphs():
    """Build the same graph remotely and locally.

    Yields (remote_graph, local_graph). Nodes a/b/c (a,b typed),
    edge a->b in two layers (L1@1, L2@2), b->c@3 (default), c->a@4.
    """
    from raphtory import Graph

    with _remote_graph("cg") as rg:

        lg = Graph()
        for g, add_node, add_edge in (
            (rg, rg.add_node, rg.add_edge),
            (lg, lg.add_node, lg.add_edge),
        ):
            add_node(1, "a", node_type="T1")
            add_node(2, "b", node_type="T2")
            add_node(3, "c")
            add_edge(1, "a", "b", layer="L1")
            add_edge(2, "a", "b", layer="L2")
            add_edge(3, "b", "c")
            add_edge(4, "c", "a")
        yield rg, lg


def _ts(events):
    """Map a list of (Remote)EventTime|None to their timestamps.

    Both the local `EventTime` and the `RemoteEventTime` expose `.t`.
    """
    return [None if e is None else e.t for e in events]


def test_nodes_columnar_accessors():
    """`RemoteNodes.name` / `.node_type` / `.id` mirror local `Nodes`."""
    with _make_columnar_graphs() as (rg, lg):
        nodes = rg.nodes
        rids = nodes.id
        # id matches local (string GIDs over the transport).
        assert set(rids) == {str(i) for i in lg.nodes.id}

        # Keyed by id so element order is irrelevant.
        rmap_name = dict(zip(rids, nodes.name))
        lmap_name = dict(zip([str(i) for i in lg.nodes.id], list(lg.nodes.name)))
        assert rmap_name == lmap_name == {"a": "a", "b": "b", "c": "c"}

        rmap_type = dict(zip(rids, nodes.node_type))
        lmap_type = dict(zip([str(i) for i in lg.nodes.id], list(lg.nodes.node_type)))
        assert rmap_type == lmap_type == {"a": "T1", "b": "T2", "c": None}


def test_path_from_node_columnar_accessors():
    """`RemotePathFromNode.name` / `.node_type` / `.id` mirror local
    `PathFromNode` (flat, one value per neighbour)."""
    with _make_columnar_graphs() as (rg, lg):
        # a's neighbours: b (a->b) and c (c->a).
        rpath = rg.node("a").neighbours
        lpath = lg.node("a").neighbours

        assert sorted(rpath.id) == sorted(str(i) for i in lpath.id)
        assert sorted(rpath.name) == sorted(lpath.name) == ["b", "c"]

        rmap = dict(zip(rpath.id, rpath.node_type))
        lmap = dict(zip([str(i) for i in lpath.id], list(lpath.node_type)))
        assert rmap == lmap == {"b": "T2", "c": None}


def test_path_from_graph_columnar_accessors():
    """`RemotePathFromGraph.name` / `.node_type` / `.id` mirror local
    `PathFromGraph` (nested, per-source lists)."""
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        rpath = rg.nodes.neighbours

        # Per-source, keyed by source id (order within a source is not
        # guaranteed, so sort each inner list).
        r_names = {s: sorted(inner) for s, inner in zip(src_ids, rpath.name)}
        r_ids = {s: sorted(inner) for s, inner in zip(src_ids, rpath.id)}
        r_types = {
            s: sorted(inner, key=lambda x: (x is None, x))
            for s, inner in zip(src_ids, rpath.node_type)
        }

        lsrc = [str(i) for i in lg.nodes.id]
        lpath = lg.nodes.neighbours
        l_names = {
            s: sorted(inner) for s, inner in zip(lsrc, [list(x) for x in lpath.name])
        }
        l_ids = {
            s: sorted(str(i) for i in inner)
            for s, inner in zip(lsrc, [list(x) for x in lpath.id])
        }
        l_types = {
            s: sorted(list(inner), key=lambda x: (x is None, x))
            for s, inner in zip(lsrc, [list(x) for x in lpath.node_type])
        }

        assert r_names == l_names
        assert r_ids == l_ids
        assert r_types == l_types
        # Sanity: a neighbours b and c.
        assert r_names["a"] == ["b", "c"]


def test_edges_columnar_accessors():
    """`RemoteEdges.id` / `.layer_names` / `.earliest_time` / `.latest_time`
    mirror local `Edges`."""
    with _make_columnar_graphs() as (rg, lg):
        redges = rg.edges
        rids = redges.id
        # id: list of (src, dst) tuples — 3 unique edges.
        assert (
            sorted(rids) == sorted(lg.edges.id) == [("a", "b"), ("b", "c"), ("c", "a")]
        )

        # layer_names keyed by edge id.
        r_layers = {e: sorted(ls) for e, ls in zip(rids, redges.layer_names)}
        l_layers = {
            e: sorted(ls)
            for e, ls in zip(lg.edges.id, [list(x) for x in lg.edges.layer_names])
        }
        assert r_layers == l_layers
        assert r_layers[("a", "b")] == ["L1", "L2"]

        # earliest / latest time keyed by edge id.
        r_early = dict(zip(rids, _ts(redges.earliest_time)))
        l_early = dict(zip(lg.edges.id, _ts(list(lg.edges.earliest_time))))
        assert r_early == l_early
        assert r_early[("a", "b")] == 1

        r_late = dict(zip(rids, _ts(redges.latest_time)))
        l_late = dict(zip(lg.edges.id, _ts(list(lg.edges.latest_time))))
        assert r_late == l_late
        assert r_late[("a", "b")] == 2


def test_edges_columnar_exploded_layer_name_and_time():
    """`RemoteEdges.layer_name` / `.time` on exploded edges mirror local."""
    with _make_columnar_graphs() as (rg, lg):
        rexpl = rg.edges.explode()
        lexpl = lg.edges.explode()

        # Build sorted (src, dst, layer_name, timestamp) tuples for comparison.
        r_rows = sorted(
            (src, dst, ln, t.t)
            for (src, dst), ln, t in zip(rexpl.id, rexpl.layer_name, rexpl.time)
        )
        l_rows = sorted(
            (src, dst, ln, t.t)
            for (src, dst), ln, t in zip(
                lexpl.id, list(lexpl.layer_name), list(lexpl.time)
            )
        )
        assert r_rows == l_rows
        # 4 exploded events: (a,b,L1,1),(a,b,L2,2),(b,c,_default,3),(c,a,_default,4)
        assert ("a", "b", "L1", 1) in r_rows
        assert ("a", "b", "L2", 2) in r_rows


def test_nested_edges_columnar_accessors():
    """`RemoteNestedEdges.id` / `.layer_names` / `.earliest_time` mirror local
    `NestedEdges` (nested, per-source lists)."""
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        rne = rg.nodes.edges  # incident edges per node

        r_ids = {s: sorted(inner) for s, inner in zip(src_ids, rne.id)}
        r_layers = {
            s: sorted(sorted(ls) for ls in inner)
            for s, inner in zip(src_ids, rne.layer_names)
        }
        r_early = {
            s: sorted(_ts(inner)) for s, inner in zip(src_ids, rne.earliest_time)
        }

        lsrc = [str(i) for i in lg.nodes.id]
        lne = lg.nodes.edges
        l_ids = {s: sorted(inner) for s, inner in zip(lsrc, [list(x) for x in lne.id])}
        l_layers = {
            s: sorted(sorted(list(ls)) for ls in inner)
            for s, inner in zip(lsrc, [list(x) for x in lne.layer_names])
        }
        l_early = {
            s: sorted(_ts(list(inner)))
            for s, inner in zip(lsrc, [list(x) for x in lne.earliest_time])
        }

        assert r_ids == l_ids
        assert r_layers == l_layers
        assert r_early == l_early
        # a is incident to (a,b) and (c,a).
        assert r_ids["a"] == [("a", "b"), ("c", "a")]


def test_nested_edges_columnar_exploded_layer_name_and_time():
    """`RemoteNestedEdges.explode().layer_name` / `.time` mirror local
    `NestedEdges.explode()`, keyed per source node."""
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        rexpl = rg.nodes.edges.explode()
        lexpl = lg.nodes.edges.explode()

        # Per-source sorted (src, dst, layer_name, timestamp) rows.
        r_rows = {
            s: sorted((src, dst, ln, t.t) for (src, dst), ln, t in zip(ids, lns, ts))
            for s, ids, lns, ts in zip(src_ids, rexpl.id, rexpl.layer_name, rexpl.time)
        }

        lsrc = [str(i) for i in lg.nodes.id]
        l_rows = {
            s: sorted(
                (src, dst, ln, t.t)
                for (src, dst), ln, t in zip(list(ids), list(lns), list(ts))
            )
            for s, ids, lns, ts in zip(
                lsrc,
                [list(x) for x in lexpl.id],
                [list(x) for x in lexpl.layer_name],
                [list(x) for x in lexpl.time],
            )
        }

        assert r_rows == l_rows
        # Source a is incident to a->b (L1@1, L2@2) and c->a (@4).
        assert r_rows["a"] == sorted(
            [("a", "b", "L1", 1), ("a", "b", "L2", 2), ("c", "a", "_default", 4)]
        )


def test_nested_edges_columnar_layer_name_requires_explode():
    """`NestedEdges.layer_name` raises before explode — same specific error in
    both APIs (message mentions `layer_name`)."""
    with _make_columnar_graphs() as (rg, lg):
        with pytest.raises(Exception, match="layer_name"):
            list(lg.nodes.edges.layer_name)
        # Remote surfaces the same server-side error, also mentioning layer_name.
        with pytest.raises(Exception, match="layer_name"):
            rg.nodes.edges.layer_name


def _layer_rows(coll):
    """(src, dst, layer_name, time-availability) per member — `.time` is
    expected to raise on a layer-exploded edge in both APIs."""
    out = []
    for e in coll:
        try:
            e.time
            t = "no-raise"
        except Exception:
            t = "raises"
        out.append((e.src.name, e.dst.name, e.layer_name, t))
    return sorted(out)


def test_edges_explode_layers_collect_pins_layers():
    """`edges.explode_layers().collect()` handles are pinned to their layer —
    `.src`/`.dst`/`.layer_name` match local per member and `.time` raises
    (a layer instance spans all its events, matching local). Flat + nested."""
    with _make_columnar_graphs() as (rg, lg):
        r = _layer_rows(rg.edges.explode_layers().collect())
        assert r == _layer_rows(lg.edges.explode_layers().collect())
        assert all(t == "raises" for *_, t in r)
        # nested (per source node)
        r_nested = [
            _layer_rows(inner) for inner in rg.nodes.edges.explode_layers().collect()
        ]
        l_nested = [
            _layer_rows(inner) for inner in lg.nodes.edges.explode_layers().collect()
        ]
        assert r_nested == l_nested


def test_edges_element_predicates():
    """`RemoteEdges.is_active` / `.is_valid` / `.is_deleted` / `.is_self_loop`
    mirror local `Edges` (flat `list[bool]`, keyed by edge id)."""
    with _make_columnar_graphs() as (rg, lg):
        redges = rg.edges
        rids = redges.id

        def keyed_remote(vals):
            return dict(zip(rids, vals))

        def keyed_local(vals):
            return dict(zip(lg.edges.id, list(vals)))

        r_active = keyed_remote(redges.is_active())
        r_valid = keyed_remote(redges.is_valid())
        r_deleted = keyed_remote(redges.is_deleted())
        r_self = keyed_remote(redges.is_self_loop())

        l_active = keyed_local(lg.edges.is_active())
        l_valid = keyed_local(lg.edges.is_valid())
        l_deleted = keyed_local(lg.edges.is_deleted())
        l_self = keyed_local(lg.edges.is_self_loop())

        assert r_active == l_active
        assert r_valid == l_valid
        assert r_deleted == l_deleted
        assert r_self == l_self
        # Ground truth: no self-loops, none deleted, all valid in this graph.
        assert set(r_deleted.values()) == {False}
        assert set(r_valid.values()) == {True}
        assert set(r_self.values()) == {False}


def test_nested_edges_element_predicates():
    """`RemoteNestedEdges.is_active` / `.is_valid` / `.is_deleted` /
    `.is_self_loop` mirror local `NestedEdges` (nested `list[list[bool]]`,
    keyed per source node then by edge id)."""
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        rne = rg.nodes.edges

        # Key each edge's predicate by (source, edge id) so ordering within a
        # source is irrelevant.
        def keyed_remote(vals):
            out = {}
            for s, ids, flags in zip(src_ids, rne.id, vals):
                for eid, flag in zip(ids, flags):
                    out[(s, eid)] = flag
            return out

        lsrc = [str(i) for i in lg.nodes.id]
        lne = lg.nodes.edges

        def keyed_local(vals):
            out = {}
            for s, ids, flags in zip(
                lsrc, [list(x) for x in lne.id], [list(x) for x in vals]
            ):
                for eid, flag in zip(ids, list(flags)):
                    out[(s, eid)] = flag
            return out

        assert keyed_remote(rne.is_active()) == keyed_local(lne.is_active())
        assert keyed_remote(rne.is_valid()) == keyed_local(lne.is_valid())
        assert keyed_remote(rne.is_deleted()) == keyed_local(lne.is_deleted())
        assert keyed_remote(rne.is_self_loop()) == keyed_local(lne.is_self_loop())


@contextlib.contextmanager
def _make_property_graphs():
    """Build the same property-bearing graph remotely and locally.

    Yields (remote_graph, local_graph). Nodes a/b/c with a `score`
    property (a=10, b=20, c=10) and types T1/T2/None; edges a->b, b->c (kind
    "x"), c->a (kind "y") with a float `w`.
    """
    from raphtory import Graph

    with _remote_graph("pg") as rg:

        lg = Graph()
        for add_node, add_edge in (
            (rg.add_node, rg.add_edge),
            (lg.add_node, lg.add_edge),
        ):
            add_node(1, "a", {"score": 10}, node_type="T1")
            add_node(2, "b", {"score": 20}, node_type="T2")
            add_node(3, "c", {"score": 10})
            add_edge(1, "a", "b", {"w": 1.0, "kind": "x"})
            add_edge(2, "b", "c", {"w": 2.0, "kind": "x"})
            add_edge(3, "c", "a", {"w": 1.0, "kind": "y"})
        yield rg, lg


def test_graph_find_nodes():
    """`RemoteGraph.find_nodes` mirrors local `Graph.find_nodes` — nodes whose
    latest property values match every entry in the dict."""
    with _make_property_graphs() as (rg, lg):
        r = sorted(n.name for n in rg.find_nodes({"score": 10}))
        l = sorted(n.name for n in lg.find_nodes({"score": 10}))
        assert r == l == ["a", "c"]

        # No matches.
        assert rg.find_nodes({"score": 999}) == []
        # Two-key match narrows to a single node.
        assert [n.name for n in rg.find_nodes({"score": 20})] == ["b"]


def test_graph_find_edges():
    """`RemoteGraph.find_edges` mirrors local `Graph.find_edges` — edges whose
    latest property values match every entry in the dict."""
    with _make_property_graphs() as (rg, lg):
        r_kind = sorted(e.id for e in rg.find_edges({"kind": "x"}))
        l_kind = sorted(e.id for e in lg.find_edges({"kind": "x"}))
        assert r_kind == l_kind == [("a", "b"), ("b", "c")]

        r_w = sorted(e.id for e in rg.find_edges({"w": 1.0}))
        l_w = sorted(e.id for e in lg.find_edges({"w": 1.0}))
        assert r_w == l_w == [("a", "b"), ("c", "a")]

        assert rg.find_edges({"kind": "zzz"}) == []


def test_graph_get_all_node_types():
    """`RemoteGraph.get_all_node_types` mirrors local
    `Graph.get_all_node_types`."""
    with _make_property_graphs() as (rg, lg):
        assert sorted(rg.get_all_node_types()) == sorted(lg.get_all_node_types())
        assert sorted(rg.get_all_node_types()) == ["T1", "T2"]


def test_properties_get_dtype_of():
    """`RemoteProperties.get_dtype_of` mirrors local `Properties.get_dtype_of`
    (the local `PropType` compares equal to the returned string)."""
    with _make_property_graphs() as (rg, lg):
        # Node property dtype.
        r_np = rg.node("a").properties
        l_np = lg.node("a").properties
        assert l_np.get_dtype_of("score") == r_np.get_dtype_of("score")
        assert r_np.get_dtype_of("score") == "I64"
        # Missing key -> None on both.
        assert r_np.get_dtype_of("nope") is None
        assert l_np.get_dtype_of("nope") is None

        # Edge property dtypes (float + string).
        r_ep = rg.edge("a", "b").properties
        l_ep = lg.edge("a", "b").properties
        assert l_ep.get_dtype_of("w") == r_ep.get_dtype_of("w") == "F64"
        assert l_ep.get_dtype_of("kind") == r_ep.get_dtype_of("kind") == "Str"


def test_edges_src_dst_nbr():
    """`RemoteEdges.src` / `.dst` / `.nbr` return a flat `RemotePathFromNode`
    whose columnar accessors mirror the local `Edges.src` / `.dst` / `.nbr`.

    Keyed by edge id so element order is irrelevant.
    """
    with _make_columnar_graphs() as (rg, lg):
        redges = rg.edges
        rids = redges.id  # list[(str, str)]
        # Local edge ids are already string tuples for string-named nodes.
        lids = list(lg.edges.id)

        r_src = dict(zip(rids, redges.src.name))
        r_dst = dict(zip(rids, redges.dst.name))
        r_nbr = dict(zip(rids, redges.nbr.name))
        l_src = dict(zip(lids, list(lg.edges.src.name)))
        l_dst = dict(zip(lids, list(lg.edges.dst.name)))
        l_nbr = dict(zip(lids, list(lg.edges.nbr.name)))

        assert r_src == l_src
        assert r_dst == l_dst
        assert r_nbr == l_nbr
        # Ground truth: src is the first endpoint; dst / nbr the second (all
        # edges here are traversed as out-edges, so nbr == dst).
        assert r_src == {("a", "b"): "a", ("b", "c"): "b", ("c", "a"): "c"}
        assert r_dst == {("a", "b"): "b", ("b", "c"): "c", ("c", "a"): "a"}
        assert r_nbr == r_dst

        # id parity on the src path (node GIDs stringified over the transport).
        r_src_ids = dict(zip(rids, redges.src.id))
        l_src_ids = dict(zip(lids, [str(i) for i in lg.edges.src.id]))
        assert r_src_ids == l_src_ids

        # node_type parity on the src path.
        r_src_types = dict(zip(rids, redges.src.node_type))
        l_src_types = dict(zip(lids, list(lg.edges.src.node_type)))
        assert r_src_types == l_src_types
        assert r_src_types == {("a", "b"): "T1", ("b", "c"): "T2", ("c", "a"): None}


def test_edges_src_neighbours_composition():
    """`rg.edges.src` returns a real `RemotePathFromNode` — chaining a further
    hop (`.neighbours.name`) works and mirrors the local API."""
    with _make_columnar_graphs() as (rg, lg):
        r = sorted(rg.edges.src.neighbours.name)
        l = sorted(lg.edges.src.neighbours.name)
        assert r == l
        assert len(r) > 0


def test_nested_edges_src_dst_nbr():
    """`RemoteNestedEdges.src` / `.dst` / `.nbr` return a nested
    `RemotePathFromGraph` whose columnar accessors mirror the local
    `NestedEdges.src` / `.dst` / `.nbr`.

    Keyed by (source node id, edge id) so ordering within a source is
    irrelevant.
    """
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        rne = rg.nodes.edges

        def keyed_remote(field_vals):
            out = {}
            for s, ids, vals in zip(src_ids, rne.id, field_vals):
                for eid, v in zip(ids, vals):
                    out[(s, eid)] = v
            return out

        lsrc = [str(i) for i in lg.nodes.id]
        lne = lg.nodes.edges

        def keyed_local(field_vals):
            out = {}
            for s, ids, vals in zip(
                lsrc, [list(x) for x in lne.id], [list(x) for x in field_vals]
            ):
                for eid, v in zip(ids, list(vals)):
                    out[(s, eid)] = v
            return out

        assert keyed_remote(rne.src.name) == keyed_local(lne.src.name)
        assert keyed_remote(rne.dst.name) == keyed_local(lne.dst.name)
        # For nested edges `nbr` is anchor-relative (the neighbour reached from
        # the source node), so it differs from `dst` on incoming edges — the
        # local parity check above is the ground truth.
        assert keyed_remote(rne.nbr.name) == keyed_local(lne.nbr.name)

        # node_type parity on the nested src path.
        assert keyed_remote(rne.src.node_type) == keyed_local(lne.src.node_type)

        # Sanity: node a participates in a->b and c->a, so its incident edges'
        # sources are {a, c}.
        by_source = {}
        for s, ids, names in zip(src_ids, rne.id, rne.src.name):
            by_source[s] = sorted(names)
        assert by_source["a"] == ["a", "c"]


def test_nested_edges_src_neighbours_composition():
    """`rg.nodes.edges.src` returns a real `RemotePathFromGraph` — chaining a
    further hop (`.neighbours.name`) works and mirrors the local API."""
    with _make_columnar_graphs() as (rg, lg):
        src_ids = rg.nodes.id
        r = {s: sorted(x) for s, x in zip(src_ids, rg.nodes.edges.src.neighbours.name)}
        lsrc = [str(i) for i in lg.nodes.id]
        l = {
            s: sorted(x)
            for s, x in zip(
                lsrc, [list(inner) for inner in lg.nodes.edges.src.neighbours.name]
            )
        }
        assert r == l


# ============================================================================
# Collection-level earliest_time / latest_time (getters), default_layer
# (method), and columnar metadata / properties views — parity with local.
# ============================================================================


def _descriptor_kind(cls, name):
    """Return "getter", "method", or "missing" for attribute `name` on `cls`.

    PyO3 `#[getter]`s surface as `getset_descriptor`; plain `#[pymethods]` as
    `method_descriptor`. Used to assert the remote descriptor kind matches the
    local one (property-vs-method parity).
    """
    for base in cls.__mro__:
        if name in base.__dict__:
            tn = type(base.__dict__[name]).__name__
            return "getter" if tn in ("getset_descriptor", "property") else "method"
    return "missing"


@contextlib.contextmanager
def _make_columnar_property_graphs():
    """Build the same property/metadata graph remotely and locally.

    Yields (remote_graph, local_graph). Nodes a/b/c with temporal
    property `p` (a=10, b=20, c none) and metadata `m` (a=1 only). Edges
    a->b (prop w=5, metadata em=9), a->c (prop w=7), b->c (none).
    """
    from raphtory import Graph

    with _remote_graph("pg") as rg:

        lg = Graph()
        for g in (rg, lg):
            g.add_node(1, "a", {"p": 10})
            g.add_node(1, "b", {"p": 20})
            g.add_node(1, "c")
            g.node("a").add_metadata({"m": 1})
            g.add_edge(1, "a", "b", {"w": 5})
            g.add_edge(2, "a", "c", {"w": 7})
            g.add_edge(3, "b", "c")
            g.edge("a", "b").add_metadata({"em": 9})
        yield rg, lg


def test_nodes_earliest_latest_time_getters():
    """`RemoteNodes.earliest_time` / `.latest_time` are getters returning a
    flat per-node column, matching local `Nodes`."""
    with _make_columnar_graphs() as (rg, lg):
        from raphtory import Nodes

        assert _descriptor_kind(type(rg.nodes), "earliest_time") == "getter"
        assert _descriptor_kind(Nodes, "earliest_time") == "getter"
        assert _descriptor_kind(type(rg.nodes), "latest_time") == "getter"
        assert _descriptor_kind(Nodes, "latest_time") == "getter"

        rids = rg.nodes.id
        lids = [str(i) for i in lg.nodes.id]

        r_early = dict(zip(rids, _ts(rg.nodes.earliest_time)))
        l_early = dict(zip(lids, _ts(list(lg.nodes.earliest_time))))
        assert r_early == l_early
        # a/b added at t=1; c added as a node at t=3.
        assert r_early == {"a": 1, "b": 1, "c": 3}

        r_late = dict(zip(rids, _ts(rg.nodes.latest_time)))
        l_late = dict(zip(lids, _ts(list(lg.nodes.latest_time))))
        assert r_late == l_late


def test_path_from_node_earliest_latest_time_getters():
    """`RemotePathFromNode.earliest_time` / `.latest_time` are getters
    returning a flat per-node column, matching local `PathFromNode`."""
    with _make_columnar_graphs() as (rg, lg):
        from raphtory import PathFromNode

        assert (
            _descriptor_kind(type(rg.node("a").neighbours), "earliest_time") == "getter"
        )
        assert _descriptor_kind(PathFromNode, "earliest_time") == "getter"
        assert (
            _descriptor_kind(type(rg.node("a").neighbours), "latest_time") == "getter"
        )
        assert _descriptor_kind(PathFromNode, "latest_time") == "getter"

        rpath = rg.node("a").neighbours
        lpath = lg.node("a").neighbours
        rids = rpath.id
        lids = [str(i) for i in lpath.id]

        assert dict(zip(rids, _ts(rpath.earliest_time))) == dict(
            zip(lids, _ts(list(lpath.earliest_time)))
        )
        assert dict(zip(rids, _ts(rpath.latest_time))) == dict(
            zip(lids, _ts(list(lpath.latest_time)))
        )


def test_path_from_graph_earliest_latest_time_getters():
    """`RemotePathFromGraph.earliest_time` / `.latest_time` are getters
    returning a nested per-source column, matching local `PathFromGraph`."""
    with _make_columnar_graphs() as (rg, lg):
        from raphtory import PathFromGraph

        assert _descriptor_kind(type(rg.nodes.neighbours), "earliest_time") == "getter"
        assert _descriptor_kind(PathFromGraph, "earliest_time") == "getter"
        assert _descriptor_kind(type(rg.nodes.neighbours), "latest_time") == "getter"
        assert _descriptor_kind(PathFromGraph, "latest_time") == "getter"

        r_src = rg.nodes.id
        l_src = [str(i) for i in lg.nodes.id]
        rpath = rg.nodes.neighbours
        lpath = lg.nodes.neighbours

        # Per source, key inner values by neighbour id (inner order not fixed).
        r_early = {
            s: dict(zip(nbrs, _ts(vals)))
            for s, nbrs, vals in zip(r_src, rpath.id, rpath.earliest_time)
        }
        l_early = {
            s: dict(zip([str(i) for i in nbrs], _ts(list(vals))))
            for s, nbrs, vals in zip(l_src, lpath.id, list(lpath.earliest_time))
        }
        assert r_early == l_early

        r_late = {
            s: dict(zip(nbrs, _ts(vals)))
            for s, nbrs, vals in zip(r_src, rpath.id, rpath.latest_time)
        }
        l_late = {
            s: dict(zip([str(i) for i in nbrs], _ts(list(vals))))
            for s, nbrs, vals in zip(l_src, lpath.id, list(lpath.latest_time))
        }
        assert r_late == l_late


def test_collections_default_layer_is_method():
    """`default_layer()` is a method on all five remote collections, returns
    the same collection type, and restricts to the default layer — matching
    local (which also exposes it as a method)."""
    with _make_columnar_graphs() as (rg, lg):
        from raphtory import Nodes, Edges, PathFromNode, PathFromGraph, NestedEdges

        pairs = [
            (rg.nodes, Nodes, "RemoteNodes"),
            (rg.edges, Edges, "RemoteEdges"),
            (rg.node("a").neighbours, PathFromNode, "RemotePathFromNode"),
            (rg.nodes.neighbours, PathFromGraph, "RemotePathFromGraph"),
            (rg.nodes.edges, NestedEdges, "RemoteNestedEdges"),
        ]
        for remote_coll, local_cls, tyname in pairs:
            assert _descriptor_kind(type(remote_coll), "default_layer") == "method"
            assert _descriptor_kind(local_cls, "default_layer") == "method"
            assert type(remote_coll.default_layer()).__name__ == tyname

        # `default_layer` is a view op — it restricts the visible events, not
        # collection membership; the remote edge id set matches local exactly.
        assert sorted(rg.edges.default_layer().id) == sorted(
            lg.edges.default_layer().id
        )
        # Its earliest_time column matches local under the default-layer view
        # (a->b has no default-layer events, so None on both).
        r_early = dict(
            zip(
                rg.edges.default_layer().id, _ts(rg.edges.default_layer().earliest_time)
            )
        )
        l_early = dict(
            zip(
                lg.edges.default_layer().id,
                _ts(list(lg.edges.default_layer().earliest_time)),
            )
        )
        assert r_early == l_early


def _assert_view_internally_consistent(view):
    """A columnar view's keys/values/items/as_dict must agree with get()."""
    keys = list(view.keys())
    d = view.as_dict()
    assert set(d.keys()) == set(keys)
    for k in keys:
        assert d[k] == view.get(k)
    assert list(view.values()) == [d[k] for k in keys]
    assert list(view.items()) == [(k, d[k]) for k in keys]


def test_nodes_metadata_properties_view():
    """`RemoteNodes.metadata` / `.properties` are getters returning columnar
    views whose get/keys/values/items/as_dict mirror local `Nodes`."""
    with _make_columnar_property_graphs() as (rg, lg):
        from raphtory import Nodes

        for name in ("metadata", "properties"):
            assert _descriptor_kind(type(rg.nodes), name) == "getter"
            assert _descriptor_kind(Nodes, name) == "getter"

        rids = rg.nodes.id
        lids = [str(i) for i in lg.nodes.id]

        # metadata.get('m') — column, one per node, None where absent.
        assert dict(zip(rids, rg.nodes.metadata.get("m"))) == dict(
            zip(lids, list(lg.nodes.metadata.get("m")))
        )
        assert dict(zip(rids, rg.nodes.metadata.get("m"))) == {
            "a": 1,
            "b": None,
            "c": None,
        }
        # properties.get('p') — latest temporal value per node.
        assert dict(zip(rids, rg.nodes.properties.get("p"))) == dict(
            zip(lids, list(lg.nodes.properties.get("p")))
        )
        assert dict(zip(rids, rg.nodes.properties.get("p"))) == {
            "a": 10,
            "b": 20,
            "c": None,
        }
        # keys parity (as sets — key ordering is not contractually stable).
        assert set(rg.nodes.metadata.keys()) == set(lg.nodes.metadata.keys()) == {"m"}
        assert (
            set(rg.nodes.properties.keys()) == set(lg.nodes.properties.keys()) == {"p"}
        )
        # get() on an absent key returns None on both, matching local.
        assert rg.nodes.metadata.get("nope") is None
        assert lg.nodes.metadata.get("nope") is None

        _assert_view_internally_consistent(rg.nodes.metadata)
        _assert_view_internally_consistent(rg.nodes.properties)


def test_edges_metadata_properties_view():
    """`RemoteEdges.metadata` / `.properties` mirror local `Edges` (flat)."""
    with _make_columnar_property_graphs() as (rg, lg):
        from raphtory import Edges

        for name in ("metadata", "properties"):
            assert _descriptor_kind(type(rg.edges), name) == "getter"
            assert _descriptor_kind(Edges, name) == "getter"

        rids = rg.edges.id
        lids = list(lg.edges.id)

        assert dict(zip(rids, rg.edges.metadata.get("em"))) == dict(
            zip(lids, list(lg.edges.metadata.get("em")))
        )
        assert dict(zip(rids, rg.edges.metadata.get("em")))[("a", "b")] == 9
        assert dict(zip(rids, rg.edges.properties.get("w"))) == dict(
            zip(lids, list(lg.edges.properties.get("w")))
        )
        assert dict(zip(rids, rg.edges.properties.get("w"))) == {
            ("a", "b"): 5,
            ("a", "c"): 7,
            ("b", "c"): None,
        }
        assert (
            set(rg.edges.properties.keys()) == set(lg.edges.properties.keys()) == {"w"}
        )

        _assert_view_internally_consistent(rg.edges.metadata)
        _assert_view_internally_consistent(rg.edges.properties)


def test_path_from_node_metadata_properties_view():
    """`RemotePathFromNode.metadata` / `.properties` mirror local
    `PathFromNode` (flat, one value per neighbour)."""
    with _make_columnar_property_graphs() as (rg, lg):
        from raphtory import PathFromNode

        for name in ("metadata", "properties"):
            assert _descriptor_kind(type(rg.node("a").neighbours), name) == "getter"
            assert _descriptor_kind(PathFromNode, name) == "getter"

        # neighbours of b: a (a->b) and c (b->c) — a carries metadata m=1.
        rpath = rg.node("b").neighbours
        lpath = lg.node("b").neighbours
        rids = rpath.id
        lids = [str(i) for i in lpath.id]

        assert dict(zip(rids, rpath.properties.get("p"))) == dict(
            zip(lids, list(lpath.properties.get("p")))
        )
        # metadata m present on a only → column [a: 1, c: None].
        assert dict(zip(rids, rpath.metadata.get("m"))) == dict(
            zip(lids, list(lpath.metadata.get("m")))
        )
        assert dict(zip(rids, rpath.metadata.get("m"))) == {"a": 1, "c": None}
        _assert_view_internally_consistent(rpath.properties)
        _assert_view_internally_consistent(rpath.metadata)


def test_path_from_graph_metadata_properties_view():
    """`RemotePathFromGraph.metadata` / `.properties` mirror local
    `PathFromGraph` (nested, per-source columns)."""
    with _make_columnar_property_graphs() as (rg, lg):
        from raphtory import PathFromGraph

        for name in ("metadata", "properties"):
            assert _descriptor_kind(type(rg.nodes.neighbours), name) == "getter"
            assert _descriptor_kind(PathFromGraph, name) == "getter"

        r_src = rg.nodes.id
        l_src = [str(i) for i in lg.nodes.id]
        rpath = rg.nodes.neighbours
        lpath = lg.nodes.neighbours

        # properties.get('p'): nested column, keyed per source then per nbr id.
        r_p = {
            s: dict(zip(nbrs, vals))
            for s, nbrs, vals in zip(r_src, rpath.id, rpath.properties.get("p"))
        }
        l_p = {
            s: dict(zip([str(i) for i in nbrs], list(vals)))
            for s, nbrs, vals in zip(l_src, lpath.id, list(lpath.properties.get("p")))
        }
        assert r_p == l_p

        r_m = {
            s: dict(zip(nbrs, vals))
            for s, nbrs, vals in zip(r_src, rpath.id, rpath.metadata.get("m"))
        }
        l_m = {
            s: dict(zip([str(i) for i in nbrs], list(vals)))
            for s, nbrs, vals in zip(l_src, lpath.id, list(lpath.metadata.get("m")))
        }
        assert r_m == l_m

        assert set(rg.nodes.neighbours.properties.keys()) == set(
            lg.nodes.neighbours.properties.keys()
        )
        _assert_view_internally_consistent(rpath.properties)
        _assert_view_internally_consistent(rpath.metadata)


def test_nested_edges_metadata_properties_view():
    """`RemoteNestedEdges.metadata` / `.properties` mirror local `NestedEdges`
    (nested, per-source columns)."""
    with _make_columnar_property_graphs() as (rg, lg):
        from raphtory import NestedEdges

        for name in ("metadata", "properties"):
            assert _descriptor_kind(type(rg.nodes.edges), name) == "getter"
            assert _descriptor_kind(NestedEdges, name) == "getter"

        r_src = rg.nodes.id
        l_src = [str(i) for i in lg.nodes.id]
        rne = rg.nodes.edges
        lne = lg.nodes.edges

        # properties.get('w'): per source, keyed by edge id (src,dst).
        r_w = {
            s: dict(zip(eids, vals))
            for s, eids, vals in zip(r_src, rne.id, rne.properties.get("w"))
        }
        l_w = {
            s: dict(zip(eids, list(vals)))
            for s, eids, vals in zip(l_src, lne.id, list(lne.properties.get("w")))
        }
        assert r_w == l_w

        r_em = {
            s: dict(zip(eids, vals))
            for s, eids, vals in zip(r_src, rne.id, rne.metadata.get("em"))
        }
        l_em = {
            s: dict(zip(eids, list(vals)))
            for s, eids, vals in zip(l_src, lne.id, list(lne.metadata.get("em")))
        }
        assert r_em == l_em

        _assert_view_internally_consistent(rne.properties)
        _assert_view_internally_consistent(rne.metadata)


# ============ String-escaping round-trip (drop-in parity) ============
# User-supplied strings (node names, property keys, filter values) are spliced
# into GraphQL queries and must survive an ids -> node round-trip byte-for-byte.
# Each name below breaks a naively-quoted query: a bare double-quote, a
# backslash, a newline, non-ASCII unicode, and a control char (BEL, U+0007).
@pytest.mark.parametrize(
    "name",
    ['O"Brien', "back\\slash", "multi\nline", "🌟", "a\x07b"],
)
def test_special_chars_roundtrip(name):
    """A tricky node name + a quoted property key must round-trip identically
    on the remote graph and match a local twin."""
    from raphtory import Graph
    from raphtory.filter import Node

    quoted_key = 'k"ey'
    expected_val = "value"

    # Local twin graph — the source of truth for expected behaviour.
    lg = Graph()
    lg.add_node(1, name, properties={quoted_key: expected_val})
    lg.add_node(2, "anchor")
    lg.add_edge(3, name, "anchor")

    with _remote_graph("escape-graph") as rg:
        rg.add_node(1, name, properties={quoted_key: expected_val})
        rg.add_node(2, "anchor")
        rg.add_edge(3, name, "anchor")

        # Collection names must match the local twin (order-independent).
        assert sorted(rg.nodes.name) == sorted(lg.nodes.name)
        assert name in list(rg.nodes.name)

        # ids -> node round-trip: `.node(name)` validates via hasNode (the name
        # is escaped into the query) and the degree must match the local twin.
        assert rg.node(name).degree() == lg.node(name).degree() == 1

        # Property whose KEY also contains a quote — escaped into `get(key: ...)`.
        assert rg.node(name).properties.get(quoted_key) == expected_val
        assert rg.node(name).properties.get(quoted_key) == lg.node(name).properties.get(
            quoted_key
        )

        # A filter carrying the tricky value must resolve it correctly. `.filter`
        # is the drop-in-parity method; read the result columnarly (`.id`, which
        # keeps the filter in the query expression) rather than materializing
        # handles, so this stays a pure filter-value escaping check.
        assert (
            rg.nodes.filter(Node.name() == name).id
            == lg.nodes.filter(Node.name() == name).id
        )


def test_graph_view_filter_expression_remote():
    """`filter.Graph.*` expressions (graph-level view restrictions) carry to
    the server through the filter tree export — parity with local
    `Graph.filter`, including chained view ops."""
    from raphtory import Graph
    from raphtory import filter as flt

    with _remote_graph("g") as rg:
        lg = Graph()
        for g in (rg, lg):
            g.add_edge(1, "a", "b", layer="L1")
            g.add_edge(5, "b", "c", layer="L2")
            g.add_edge(9, "c", "a", layer="L1")

        for expr in (
            flt.Graph.window(0, 6),
            flt.Graph.window(0, 6).layer("L1"),
            flt.Graph.at(5),
        ):
            local_ids = sorted(lg.filter(expr).nodes.id)
            remote_ids = sorted(rg.filter(expr).nodes.id)
            assert remote_ids == local_ids, f"{expr}: {remote_ids} != {local_ids}"


def test_mixed_kind_filter_expression_remote():
    """A node∧edge expression exports structurally (no single composite kind
    can hold it) and evaluates with intersection semantics — parity with
    local `Graph.filter(node_expr & edge_expr)`."""
    from raphtory import Graph
    from raphtory import filter as flt

    with _remote_graph("g") as rg:
        lg = Graph()
        for g in (rg, lg):
            g.add_node(1, "a", properties={"score": 10})
            g.add_node(1, "b", properties={"score": 20})
            g.add_node(1, "c", properties={"score": 30})
            g.add_edge(2, "a", "b", properties={"w": 1})
            g.add_edge(3, "b", "c", properties={"w": 5})

        expr = (flt.Node.property("score") > 15) & (flt.Edge.property("w") > 2)
        local_ids = sorted(lg.filter(expr).nodes.id)
        remote_ids = sorted(rg.filter(expr).nodes.id)
        assert remote_ids == local_ids, f"{remote_ids} != {local_ids}"
