"""Transport-contract tests: the RPC counts the remote client documents.

Unlike its siblings, this module is not a local/remote parity suite — the
local ``Graph`` never appears. What it pins is the *documented transport
behaviour* of the remote client: every view op claims "Lazy — no RPC.",
every terminal and write claims "Fires one RPC.", and a handful of documented
exceptions claim more (``TemporalProperty.items()`` is two RPCs,
``TemporalProperties.histories()`` is ``1 + 2·P``, ...). Those claims are the
API's performance contract, and no value-comparing test can check them — a
client that fired three round trips per terminal would still answer
correctly. So each call here runs behind a counting HTTP proxy (see ``_rpc``)
and its round trips are counted at the wire.

The module lives in the parity directory because it shares the harness style
and the same drop-in surface, and because a documented count is itself a
parity claim: the docstring is the spec, the wire is the implementation.

Every ``expected`` value below restates a docstring in
``raphtory-graphql/src/python/client/*.rs`` — when one of these fails, either
the transport regressed or the docstring lies, and both are findings.
"""

import itertools

import pytest

from _rpc import counting_remote_graph
from raphtory import filter as f
from raphtory.graphql import RemoteEdgeAddition, RemoteNodeAddition, RemoteUpdate


def _build(g):
    """A small graph every category below can read something real from.

    Two layers plus the default, a node with *two* updates to its temporal
    ``score`` (so temporal-property terminals have a history to aggregate),
    metadata at node / edge / graph level, and a graph temporal property.
    """
    g.add_node(1, "a", node_type="person", properties={"score": 1.0})
    g.add_node(2, "a", properties={"score": 2.0})
    g.add_node(2, "b", node_type="person", properties={"score": 3.0})
    g.add_node(3, "c")
    g.add_edge(3, "a", "b", properties={"weight": 1.0}, layer="knows")
    g.add_edge(4, "b", "c", layer="works")
    g.add_edge(5, "a", "c")
    g.add_edge(6, "a", "b", properties={"weight": 2.0}, layer="knows")
    g.node("a").add_metadata({"country": "uk"})
    g.edge("a", "b").add_metadata({"kind": "friend"}, layer="knows")
    g.add_properties(1, {"gname": "counting"})
    g.add_metadata({"gmeta": "x"})


@pytest.fixture(scope="module")
def counted():
    # One server + proxy for the whole module. Write cases below mutate the
    # graph, but only with entities of their own — and an RPC *count* does not
    # depend on graph content, so reads and writes can share the fixture.
    with counting_remote_graph(_build) as (remote, counter):
        yield remote, counter


def _expect(counter, expected, label):
    assert (
        counter.value == expected
    ), f"{label}: documented {expected} RPC(s), wire saw {counter.value}"


# --- proxy sanity -------------------------------------------------------------


def test_proxy_counts_a_known_call(counted):
    """One known terminal registers exactly one POST — the meter itself works."""
    rg, counter = counted
    counter.reset()
    assert rg.count_nodes() >= 3  # a, b, c (+ any write-case leftovers)
    _expect(counter, 1, "count_nodes")
    counter.reset()
    _expect(counter, 0, "reset")


# --- 1. view ops are free ------------------------------------------------------

# Every handle type × a representative slice of the view ops, each of whose
# docstrings claims "Lazy — no RPC.".
VIEW_OPS = {
    "window": lambda h: h.window(2, 6),
    "at": lambda h: h.at(3),
    "layer": lambda h: h.layer("knows"),
    "layers": lambda h: h.layers(["knows", "works"]),
    "snapshot_at": lambda h: h.snapshot_at(5),
    "shrink_window": lambda h: h.shrink_window(3, 8),
    "exclude_layer": lambda h: h.exclude_layer("knows"),
    "valid_layers": lambda h: h.valid_layers(["knows"]),
    "default_layer": lambda h: h.default_layer(),
}

# Reaching `node` / `edge` fires that handle's own documented construction RPC
# (pinned in the construction table below); the counter is reset *after* the
# handle exists, so only the view op itself is on the meter.
HANDLES = {
    "graph": lambda rg: rg,
    "node": lambda rg: rg.node("a"),
    "edge": lambda rg: rg.edge("a", "b"),
    "nodes": lambda rg: rg.nodes,
    "edges": lambda rg: rg.edges,
    "path_from_node": lambda rg: rg.node("a").neighbours,
    "path_from_graph": lambda rg: rg.nodes.neighbours,
    "nested_edges": lambda rg: rg.nodes.edges,
}

_VIEW_MATRIX = list(itertools.product(sorted(HANDLES), sorted(VIEW_OPS)))


@pytest.mark.parametrize(
    "handle,op", _VIEW_MATRIX, ids=[f"{h}-{o}" for h, o in _VIEW_MATRIX]
)
def test_view_op_is_free(counted, handle, op):
    rg, counter = counted
    h = HANDLES[handle](rg)
    counter.reset()
    VIEW_OPS[op](h)
    _expect(counter, 0, f"{handle}.{op}")


# Filter application sites — building and attaching an expression is documented
# lazy everywhere it exists, including the collection subscript sugar.
FILTER_SITES = {
    "graph.filter_node_expr": (
        lambda rg: rg,
        lambda h: h.filter(f.Node.property("score") > 1.0),
    ),
    "graph.filter_edge_expr": (
        lambda rg: rg,
        lambda h: h.filter(f.Edge.property("weight") > 1.0),
    ),
    "node.filter": (
        lambda rg: rg.node("a"),
        lambda h: h.filter(f.Node.property("score") > 1.0),
    ),
    "nodes.filter": (
        lambda rg: rg.nodes,
        lambda h: h.filter(f.Node.property("score") > 1.0),
    ),
    "nodes.select": (
        lambda rg: rg.nodes,
        lambda h: h.select(f.Node.property("score") > 1.0),
    ),
    "nodes.subscript": (
        lambda rg: rg.nodes,
        lambda h: h[f.Node.property("score") > 1.0],
    ),
    "edges.filter": (
        lambda rg: rg.edges,
        lambda h: h.filter(f.Edge.property("weight") > 1.0),
    ),
    "edges.select": (
        lambda rg: rg.edges,
        lambda h: h.select(f.Edge.property("weight") > 1.0),
    ),
    "path_from_node.select": (
        lambda rg: rg.node("a").neighbours,
        lambda h: h.select(f.Node.property("score") > 1.0),
    ),
    "path_from_graph.select": (
        lambda rg: rg.nodes.neighbours,
        lambda h: h.select(f.Node.property("score") > 1.0),
    ),
}


@pytest.mark.parametrize("reach,apply", FILTER_SITES.values(), ids=list(FILTER_SITES))
def test_filter_site_is_free(counted, reach, apply):
    rg, counter = counted
    h = reach(rg)
    counter.reset()
    apply(h)
    _expect(counter, 0, "filter application")


def test_long_view_chain_is_free(counted):
    """A whole chain of view ops — across handle hops — fires nothing."""
    rg, counter = counted
    counter.reset()
    rg.window(0, 10).layer("knows").at(3)
    rg.window(0, 10).nodes.window(1, 5).valid_layers(["knows"]).neighbours
    rg.edges.window(0, 10).exclude_layer("works").explode()
    rg.node("a")  # not free — but pinned below, not here
    counter.reset()
    rg.after(1).before(9).snapshot_at(5).edges.explode_layers()
    _expect(counter, 0, "view chain")


# --- 6. handle construction ----------------------------------------------------

# (reach handle, construct, documented RPCs). `node()` / `edge()` are the two
# constructions documented to fire — one validation RPC each (hasNode /
# hasEdge); every container / traversal getter is documented lazy, and
# `temporal.get` / `temporal[k]` document one existence-check RPC.
CONSTRUCTION = {
    "graph.node": (lambda rg: rg, lambda g: g.node("a"), 1),
    "graph.node_absent": (lambda rg: rg, lambda g: g.node("nobody"), 1),
    "graph.edge": (lambda rg: rg, lambda g: g.edge("a", "b"), 1),
    "graph.nodes": (lambda rg: rg, lambda g: g.nodes, 0),
    "graph.edges": (lambda rg: rg, lambda g: g.edges, 0),
    "graph.metadata": (lambda rg: rg, lambda g: g.metadata, 0),
    "graph.properties": (lambda rg: rg, lambda g: g.properties, 0),
    "node.neighbours": (lambda rg: rg.node("a"), lambda n: n.neighbours, 0),
    "node.in_neighbours": (lambda rg: rg.node("a"), lambda n: n.in_neighbours, 0),
    "node.edges": (lambda rg: rg.node("a"), lambda n: n.edges, 0),
    "node.history": (lambda rg: rg.node("a"), lambda n: n.history, 0),
    "node.properties": (lambda rg: rg.node("a"), lambda n: n.properties, 0),
    "node.metadata": (lambda rg: rg.node("a"), lambda n: n.metadata, 0),
    "edge.src": (lambda rg: rg.edge("a", "b"), lambda e: e.src, 0),
    "edge.history": (lambda rg: rg.edge("a", "b"), lambda e: e.history, 0),
    "edge.explode": (lambda rg: rg.edge("a", "b"), lambda e: e.explode(), 0),
    "edges.src": (lambda rg: rg.edges, lambda es: es.src, 0),
    "nodes.neighbours": (lambda rg: rg.nodes, lambda ns: ns.neighbours, 0),
    "nodes.edges": (lambda rg: rg.nodes, lambda ns: ns.edges, 0),
    "nodes.properties": (lambda rg: rg.nodes, lambda ns: ns.properties, 0),
    "nodes.metadata": (lambda rg: rg.nodes, lambda ns: ns.metadata, 0),
    "properties.temporal": (
        lambda rg: rg.node("a").properties,
        lambda p: p.temporal,
        0,
    ),
    "temporal.get": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.get("score"),
        1,
    ),
    "temporal.getitem": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t["score"],
        1,
    ),
    "temporal_property.history": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.history,
        0,
    ),
    "temporal_property.key": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.key,
        0,
    ),
    "history.t": (lambda rg: rg.node("a").history, lambda h: h.t, 0),
    "history.dt": (lambda rg: rg.node("a").history, lambda h: h.dt, 0),
    "history.event_id": (lambda rg: rg.node("a").history, lambda h: h.event_id, 0),
    "history.reverse": (lambda rg: rg.node("a").history, lambda h: h.reverse(), 0),
    "history.intervals": (
        lambda rg: rg.node("a").history,
        lambda h: h.intervals,
        0,
    ),
}


@pytest.mark.parametrize(
    "reach,construct,expected", CONSTRUCTION.values(), ids=list(CONSTRUCTION)
)
def test_handle_construction_cost(counted, reach, construct, expected):
    rg, counter = counted
    h = reach(rg)
    counter.reset()
    construct(h)
    _expect(counter, expected, "handle construction")


# --- 2. terminals fire exactly one ----------------------------------------------

# (reach handle, terminal). Every entry's docstring claims "Fires one RPC.".
TERMINALS = {
    # graph
    "graph.count_nodes": (lambda rg: rg, lambda g: g.count_nodes()),
    "graph.count_edges": (lambda rg: rg, lambda g: g.count_edges()),
    "graph.earliest_time": (lambda rg: rg, lambda g: g.earliest_time),
    "graph.latest_time": (lambda rg: rg, lambda g: g.latest_time),
    "graph.start": (lambda rg: rg, lambda g: g.start),
    "graph.end": (lambda rg: rg, lambda g: g.end),
    "graph.earliest_edge_time": (lambda rg: rg, lambda g: g.earliest_edge_time()),
    "graph.latest_edge_time": (lambda rg: rg, lambda g: g.latest_edge_time()),
    "graph.unique_layers": (lambda rg: rg, lambda g: g.unique_layers),
    "graph.has_layer": (lambda rg: rg, lambda g: g.has_layer("knows")),
    "graph.has_node": (lambda rg: rg, lambda g: g.has_node("a")),
    "graph.has_edge": (lambda rg: rg, lambda g: g.has_edge("a", "b")),
    "graph.get_all_node_types": (lambda rg: rg, lambda g: g.get_all_node_types()),
    # node
    "node.degree": (lambda rg: rg.node("a"), lambda n: n.degree()),
    "node.in_degree": (lambda rg: rg.node("a"), lambda n: n.in_degree()),
    "node.out_degree": (lambda rg: rg.node("a"), lambda n: n.out_degree()),
    "node.name": (lambda rg: rg.node("a"), lambda n: n.name),
    "node.id": (lambda rg: rg.node("a"), lambda n: n.id),
    "node.node_type": (lambda rg: rg.node("a"), lambda n: n.node_type),
    "node.earliest_time": (lambda rg: rg.node("a"), lambda n: n.earliest_time),
    "node.latest_time": (lambda rg: rg.node("a"), lambda n: n.latest_time),
    "node.is_active": (lambda rg: rg.node("a"), lambda n: n.is_active()),
    "node.has_layer": (lambda rg: rg.node("a"), lambda n: n.has_layer("knows")),
    "node.edge_history_count": (
        lambda rg: rg.node("a"),
        lambda n: n.edge_history_count(),
    ),
    "node.first_update": (lambda rg: rg.node("a"), lambda n: n.first_update()),
    "node.last_update": (lambda rg: rg.node("a"), lambda n: n.last_update()),
    "node.getitem": (lambda rg: rg.node("a"), lambda n: n["score"]),
    # edge
    "edge.earliest_time": (lambda rg: rg.edge("a", "b"), lambda e: e.earliest_time),
    "edge.latest_time": (lambda rg: rg.edge("a", "b"), lambda e: e.latest_time),
    "edge.id": (lambda rg: rg.edge("a", "b"), lambda e: e.id),
    "edge.layer_names": (lambda rg: rg.edge("a", "b"), lambda e: e.layer_names),
    "edge.is_valid": (lambda rg: rg.edge("a", "b"), lambda e: e.is_valid()),
    "edge.is_deleted": (lambda rg: rg.edge("a", "b"), lambda e: e.is_deleted()),
    "edge.is_self_loop": (lambda rg: rg.edge("a", "b"), lambda e: e.is_self_loop()),
    "edge.is_active": (lambda rg: rg.edge("a", "b"), lambda e: e.is_active()),
    "edge.first_update": (lambda rg: rg.edge("a", "b"), lambda e: e.first_update()),
    "edge.last_update": (lambda rg: rg.edge("a", "b"), lambda e: e.last_update()),
    "edge.getitem": (lambda rg: rg.edge("a", "b"), lambda e: e["weight"]),
    # nodes collection
    "nodes.count": (lambda rg: rg.nodes, lambda ns: ns.count()),
    "nodes.degree": (lambda rg: rg.nodes, lambda ns: ns.degree()),
    "nodes.in_degree": (lambda rg: rg.nodes, lambda ns: ns.in_degree()),
    "nodes.out_degree": (lambda rg: rg.nodes, lambda ns: ns.out_degree()),
    "nodes.collect": (lambda rg: rg.nodes, lambda ns: ns.collect()),
    "nodes.id": (lambda rg: rg.nodes, lambda ns: ns.id),
    "nodes.name": (lambda rg: rg.nodes, lambda ns: ns.name),
    "nodes.node_type": (lambda rg: rg.nodes, lambda ns: ns.node_type),
    "nodes.earliest_time": (lambda rg: rg.nodes, lambda ns: ns.earliest_time),
    "nodes.edge_history_count": (
        lambda rg: rg.nodes,
        lambda ns: ns.edge_history_count(),
    ),
    # edges collection
    "edges.count": (lambda rg: rg.edges, lambda es: es.count()),
    "edges.collect": (lambda rg: rg.edges, lambda es: es.collect()),
    "edges.earliest_time": (lambda rg: rg.edges, lambda es: es.earliest_time),
    # paths
    "path_from_node.count": (
        lambda rg: rg.node("a").neighbours,
        lambda p: p.count(),
    ),
    "path_from_node.degree": (
        lambda rg: rg.node("a").neighbours,
        lambda p: p.degree(),
    ),
    "path_from_node.collect": (
        lambda rg: rg.node("a").neighbours,
        lambda p: p.collect(),
    ),
    "path_from_graph.len": (
        lambda rg: rg.nodes.neighbours,
        lambda p: len(p),
    ),
    "path_from_graph.degree": (
        lambda rg: rg.nodes.neighbours,
        lambda p: p.degree(),
    ),
    "path_from_graph.collect": (
        lambda rg: rg.nodes.neighbours,
        lambda p: p.collect(),
    ),
    # properties container (node + graph)
    "properties.get": (lambda rg: rg.node("a").properties, lambda p: p.get("score")),
    "properties.getitem": (lambda rg: rg.node("a").properties, lambda p: p["score"]),
    "properties.keys": (lambda rg: rg.node("a").properties, lambda p: p.keys()),
    "properties.values": (lambda rg: rg.node("a").properties, lambda p: p.values()),
    "properties.items": (lambda rg: rg.node("a").properties, lambda p: p.items()),
    "properties.get_dtype_of": (
        lambda rg: rg.node("a").properties,
        lambda p: p.get_dtype_of("score"),
    ),
    "graph_properties.get": (lambda rg: rg.properties, lambda p: p.get("gname")),
    # metadata container (node + edge + graph)
    "metadata.get": (lambda rg: rg.node("a").metadata, lambda m: m.get("country")),
    "metadata.getitem": (lambda rg: rg.node("a").metadata, lambda m: m["country"]),
    "metadata.keys": (lambda rg: rg.node("a").metadata, lambda m: m.keys()),
    "metadata.values": (lambda rg: rg.node("a").metadata, lambda m: m.values()),
    "metadata.items": (lambda rg: rg.node("a").metadata, lambda m: m.items()),
    "edge_metadata.get": (
        lambda rg: rg.edge("a", "b").metadata,
        lambda m: m.get("kind"),
    ),
    "graph_metadata.get": (lambda rg: rg.metadata, lambda m: m.get("gmeta")),
    # temporal-properties container
    "temporal.keys": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.keys(),
    ),
    "temporal.values": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.values(),
    ),
    "temporal.items": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.items(),
    ),
    # temporal-property terminals
    "temporal_property.values": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.values(),
    ),
    "temporal_property.at": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.at(2),
    ),
    "temporal_property.value": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.value(),
    ),
    "temporal_property.count": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.count(),
    ),
    "temporal_property.unique": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.unique(),
    ),
    "temporal_property.ordered_dedupe": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.ordered_dedupe(True),
    ),
    "temporal_property.sum": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.sum(),
    ),
    "temporal_property.mean": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.mean(),
    ),
    "temporal_property.min": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.min(),
    ),
    "temporal_property.max": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.max(),
    ),
    "temporal_property.median": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.median(),
    ),
    # history terminals
    "history.count": (lambda rg: rg.node("a").history, lambda h: h.count()),
    "history.is_empty": (lambda rg: rg.node("a").history, lambda h: h.is_empty()),
    "history.earliest_time": (
        lambda rg: rg.node("a").history,
        lambda h: h.earliest_time(),
    ),
    "history.latest_time": (
        lambda rg: rg.node("a").history,
        lambda h: h.latest_time(),
    ),
    "history.collect": (lambda rg: rg.node("a").history, lambda h: h.collect()),
    "history.collect_rev": (
        lambda rg: rg.node("a").history,
        lambda h: h.collect_rev(),
    ),
    "history.page": (lambda rg: rg.node("a").history, lambda h: h.page(2)),
    "history.page_rev": (lambda rg: rg.node("a").history, lambda h: h.page_rev(2)),
    "history.t.collect": (
        lambda rg: rg.node("a").history,
        lambda h: h.t.collect(),
    ),
    "edge_history.collect": (
        lambda rg: rg.edge("a", "b").history,
        lambda h: h.collect(),
    ),
}


@pytest.mark.parametrize("reach,act", TERMINALS.values(), ids=list(TERMINALS))
def test_terminal_fires_exactly_one_rpc(counted, reach, act):
    rg, counter = counted
    h = reach(rg)
    counter.reset()
    act(h)
    _expect(counter, 1, "terminal")


# --- 3. chained view + terminal is still one -------------------------------------

CHAINED = {
    "graph.window.count_edges": lambda rg: rg.window(2, 6).count_edges(),
    "graph.window.layer.count_nodes": lambda rg: rg.window(2, 6)
    .layer("knows")
    .count_nodes(),
    "nodes.window.layer.collect": lambda rg: rg.nodes.window(2, 6)
    .layer("knows")
    .collect(),
    "nodes.window.degree": lambda rg: rg.nodes.window(2, 6).degree(),
    "edges.valid_layers.count": lambda rg: rg.edges.valid_layers(["knows"]).count(),
    "graph.filter.count_nodes": lambda rg: rg.filter(
        f.Node.property("score") > 1.0
    ).count_nodes(),
    "nodes.select.collect": lambda rg: rg.nodes.select(
        f.Node.property("score") > 1.0
    ).collect(),
}


@pytest.mark.parametrize("chain", CHAINED.values(), ids=list(CHAINED))
def test_chained_view_plus_terminal_is_one_rpc(counted, chain):
    rg, counter = counted
    counter.reset()
    chain(rg)
    _expect(counter, 1, "view chain + terminal")


def test_chained_view_from_prebuilt_handle_is_one_rpc(counted):
    """The same holds when the chain hangs off a node/edge handle."""
    rg, counter = counted
    n = rg.node("a")
    e = rg.edge("a", "b")
    counter.reset()
    n.window(0, 10).layer("knows").degree()
    _expect(counter, 1, "node view chain + degree")
    counter.reset()
    e.window(0, 10).layer("knows").is_valid()
    _expect(counter, 1, "edge view chain + is_valid")


# --- 4. writes are one each --------------------------------------------------------

# (reach handle, write). Each write is one round trip; entities are unique to
# this table so cases stay order-independent.
WRITES = {
    "graph.add_node": (lambda rg: rg, lambda g: g.add_node(100, "w_node")),
    "graph.create_node": (lambda rg: rg, lambda g: g.create_node(100, "w_created")),
    "graph.add_edge": (lambda rg: rg, lambda g: g.add_edge(101, "w_src", "w_dst")),
    "graph.delete_edge": (
        lambda rg: rg,
        lambda g: g.delete_edge(102, "w_src", "w_dst"),
    ),
    "graph.add_metadata": (lambda rg: rg, lambda g: g.add_metadata({"w_gmeta": 1})),
    "graph.update_metadata": (
        lambda rg: rg,
        lambda g: g.update_metadata({"w_gmeta": 2}),
    ),
    "graph.add_properties": (
        lambda rg: rg,
        lambda g: g.add_properties(103, {"w_gprop": 1.0}),
    ),
    "node.add_updates": (
        lambda rg: rg.node("a"),
        lambda n: n.add_updates(104, {"score": 9.0}),
    ),
    "node.add_metadata": (
        lambda rg: rg.node("a"),
        lambda n: n.add_metadata({"w_nmeta": 1}),
    ),
    "node.update_metadata": (
        lambda rg: rg.node("a"),
        lambda n: n.update_metadata({"w_nmeta": 2}),
    ),
    "node.set_node_type": (
        lambda rg: rg.add_node(105, "w_typed"),
        lambda n: n.set_node_type("bot"),
    ),
    "edge.add_updates": (
        lambda rg: rg.edge("a", "b"),
        lambda e: e.add_updates(106, {"weight": 9.0}, layer="knows"),
    ),
    "edge.add_metadata": (
        lambda rg: rg.edge("a", "b"),
        lambda e: e.add_metadata({"w_emeta": 1}, layer="knows"),
    ),
    "edge.update_metadata": (
        lambda rg: rg.edge("a", "b"),
        lambda e: e.update_metadata({"w_emeta": 2}, layer="knows"),
    ),
}


@pytest.mark.parametrize("reach,write", WRITES.values(), ids=list(WRITES))
def test_write_fires_exactly_one_rpc(counted, reach, write):
    rg, counter = counted
    h = reach(rg)
    counter.reset()
    write(h)
    _expect(counter, 1, "write")


def test_batch_add_nodes_is_one_rpc(counted):
    """N node additions in one batch cross the wire exactly once."""
    rg, counter = counted
    updates = [
        RemoteNodeAddition(f"batch_n{i}", updates=[RemoteUpdate(300 + i, {"s": 1.0})])
        for i in range(5)
    ]
    counter.reset()
    rg.add_nodes(updates)
    _expect(counter, 1, "add_nodes batch of 5")


def test_batch_add_edges_is_one_rpc(counted):
    """N edge additions in one batch cross the wire exactly once."""
    rg, counter = counted
    updates = [
        RemoteEdgeAddition(
            f"batch_n{i}", f"batch_n{i + 1}", updates=[RemoteUpdate(310 + i)]
        )
        for i in range(4)
    ]
    counter.reset()
    rg.add_edges(updates)
    _expect(counter, 1, "add_edges batch of 4")


# --- 5. PathFromGraph iteration -----------------------------------------------------


def test_path_from_graph_iteration_is_one_rpc_for_the_pairing(counted):
    """`for src, path in rg.nodes.neighbours:` — one RPC fetches the source
    ids; each yielded path is a lazy handle whose own terminals pay their own
    way (documented on `RemotePathFromGraph.__iter__`)."""
    rg, counter = counted
    path = rg.nodes.neighbours
    counter.reset()
    pairs = [(source, sub) for source, sub in path]
    _expect(counter, 1, "PathFromGraph pairing")
    assert pairs, "fixture graph has nodes, the pairing cannot be empty"

    source, sub = pairs[0]
    counter.reset()
    sub.collect()
    _expect(counter, 1, "yielded path .collect()")
    counter.reset()
    source.name
    _expect(counter, 1, "yielded source .name")


def _drain(iterable):
    """Iterate with a plain ``for`` — the form the ``__iter__`` docstrings
    describe. (``list(x)`` is *not* equivalent: it first asks ``x`` for a
    length hint, which on these handles is the documented one-RPC ``__len__``
    — see ``test_list_builtin_adds_the_length_hint_len_rpc``.)"""
    return [item for item in iterable]


def test_nodes_iteration_is_one_rpc_then_one_per_yielded_terminal(counted):
    """`for n in rg.nodes:` fetches all ids in one RPC; yielded handles are
    not batched — each terminal on one fires its own (documented on
    `RemoteNodes.__iter__`)."""
    rg, counter = counted
    counter.reset()
    handles = _drain(rg.nodes)
    _expect(counter, 1, "nodes iteration")
    counter.reset()
    for n in handles:
        n.degree()
    _expect(counter, len(handles), "one RPC per yielded node terminal")


def test_edges_iteration_is_one_rpc(counted):
    rg, counter = counted
    counter.reset()
    _drain(rg.edges)
    _expect(counter, 1, "edges iteration")


def test_list_builtin_adds_the_length_hint_len_rpc(counted):
    """`list(x)` costs one more RPC than `for _ in x` on sized handles.

    Not a docstring violation — a composition of two documented costs: the
    `list()` builtin calls `__len__` for a length hint (one documented RPC)
    before `__iter__` (one documented RPC). Pinned so the extra round trip is
    a known property of `list()` rather than a surprise, and so a future
    `__length_hint__` shortcut that removes it shows up here.
    """
    rg, counter = counted
    for handle in (rg.nodes, rg.edges, rg.node("a").history):
        counter.reset()
        list(handle)
        _expect(counter, 2, f"list({type(handle).__name__})")


# --- 7. dunders fire what they document ------------------------------------------------

# (reach handle, dunder, documented RPCs). The only >1 entries are the ones
# whose docstrings *say* so: `TemporalProperty.items()` / `__iter__` compose a
# history fetch with a values fetch (two RPCs), and the whole-container
# conveniences `histories()` / `latest()` document `1 + 2·P` / `1 + P` — node
# "a" has P=1 temporal property, so 3 and 2.
DUNDERS = {
    "history.len": (lambda rg: rg.node("a").history, lambda h: len(h), 1),
    "history.getitem": (lambda rg: rg.node("a").history, lambda h: h[0], 1),
    "history.getitem_negative": (
        lambda rg: rg.node("a").history,
        lambda h: h[-1],
        1,
    ),
    "history.contains": (lambda rg: rg.node("a").history, lambda h: 1 in h, 1),
    "history.iter": (lambda rg: rg.node("a").history, lambda h: _drain(h), 1),
    "history.reversed": (
        lambda rg: rg.node("a").history,
        lambda h: list(reversed(h)),
        1,
    ),
    "history.t.len": (lambda rg: rg.node("a").history.t, lambda t: len(t), 1),
    "history.t.contains": (lambda rg: rg.node("a").history.t, lambda t: 1 in t, 1),
    "history.event_id.contains": (
        lambda rg: rg.node("a").history.event_id,
        lambda e: 0 in e,
        1,
    ),
    "history.intervals.len": (
        lambda rg: rg.node("a").history.intervals,
        lambda i: len(i),
        1,
    ),
    "history.intervals.contains": (
        lambda rg: rg.node("a").history.intervals,
        lambda i: 1 in i,
        1,
    ),
    "history.dt.contains": (
        lambda rg: rg.node("a").history.dt,
        lambda d: __import__("datetime").datetime(
            1970, 1, 1, tzinfo=__import__("datetime").timezone.utc
        )
        in d,
        1,
    ),
    # A naive datetime is not UTC-convertible, so it is simply not a member —
    # answered client-side with no wire trip at all.
    "history.dt.contains_naive": (
        lambda rg: rg.node("a").history.dt,
        lambda d: __import__("datetime").datetime(1970, 1, 1) in d,
        0,
    ),
    "nodes.len": (lambda rg: rg.nodes, lambda ns: len(ns), 1),
    "nodes.bool": (lambda rg: rg.nodes, lambda ns: bool(ns), 1),
    "edges.len": (lambda rg: rg.edges, lambda es: len(es), 1),
    "edges.bool": (lambda rg: rg.edges, lambda es: bool(es), 1),
    "properties.contains": (
        lambda rg: rg.node("a").properties,
        lambda p: "score" in p,
        1,
    ),
    "properties.len": (lambda rg: rg.node("a").properties, lambda p: len(p), 1),
    "properties.iter": (lambda rg: rg.node("a").properties, lambda p: _drain(p), 1),
    "metadata.contains": (
        lambda rg: rg.node("a").metadata,
        lambda m: "country" in m,
        1,
    ),
    "metadata.len": (lambda rg: rg.node("a").metadata, lambda m: len(m), 1),
    "metadata.iter": (lambda rg: rg.node("a").metadata, lambda m: _drain(m), 1),
    "temporal.contains": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: "score" in t,
        1,
    ),
    "temporal.len": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: len(t),
        1,
    ),
    "temporal.iter": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: _drain(t),
        1,
    ),
    "temporal_property.items": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: tp.items(),
        2,
    ),
    "temporal_property.iter": (
        lambda rg: rg.node("a").properties.temporal.get("score"),
        lambda tp: list(tp),
        2,
    ),
    "temporal.histories": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.histories(),
        3,
    ),
    "temporal.latest": (
        lambda rg: rg.node("a").properties.temporal,
        lambda t: t.latest(),
        2,
    ),
}


@pytest.mark.parametrize("reach,act,expected", DUNDERS.values(), ids=list(DUNDERS))
def test_dunder_fires_documented_rpcs(counted, reach, act, expected):
    rg, counter = counted
    h = reach(rg)
    counter.reset()
    act(h)
    _expect(counter, expected, "dunder")
