"""Local↔remote parity across entity types: Node, Edge, Nodes, Edges, PathFromNode.

Each read runs through the same ``build`` on both graphs and is asserted equal
via the shared comparator. Reads that diverge by a known gap are recorded in
``KNOWN_GAPS`` and marked ``xfail`` rather than silently skipped.
"""

import pytest

from _parity import assert_parity, graph_pair


def _build_rich(g):
    """A small multi-layer graph with types and properties, on the shared surface."""
    g.add_node(1, "a", properties={"score": 1.5}, node_type="person")
    g.add_node(2, "b", properties={"score": 2.5}, node_type="person")
    g.add_node(4, "c", node_type="org")
    g.add_edge(3, "a", "b", layer="knows")
    g.add_edge(5, "b", "c", layer="works")
    g.add_edge(7, "a", "c", layer="knows")


@pytest.fixture(scope="module")
def rich_pair():
    with graph_pair(_build_rich) as pair:
        yield pair


# --- Node -------------------------------------------------------------------

NODE_READS = [
    ("name", lambda g: g.node("a").name),
    ("id", lambda g: g.node("a").id),
    ("node_type", lambda g: g.node("a").node_type),
    ("degree", lambda g: g.node("a").degree()),
    ("in_degree", lambda g: g.node("c").in_degree()),
    ("out_degree", lambda g: g.node("a").out_degree()),
    ("earliest_time", lambda g: g.node("a").earliest_time),
    ("latest_time", lambda g: g.node("a").latest_time),
    ("neighbours", lambda g: sorted(n.name for n in g.node("a").neighbours)),
    ("out_neighbours", lambda g: sorted(n.name for n in g.node("a").out_neighbours)),
    ("in_neighbours", lambda g: sorted(n.name for n in g.node("c").in_neighbours)),
    ("edges", lambda g: sorted((e.src.name, e.dst.name) for e in g.node("a").edges)),
    ("history", lambda g: list(g.node("a").history)),
    ("prop_score", lambda g: g.node("a").properties.get("score")),
]


@pytest.mark.parametrize("name,fn", NODE_READS, ids=[c[0] for c in NODE_READS])
def test_node_read_parity(rich_pair, name, fn):
    assert_parity(rich_pair, fn)


# --- Edge -------------------------------------------------------------------

EDGE_READS = [
    ("src_dst", lambda g: (g.edge("a", "b").src.name, g.edge("a", "b").dst.name)),
    ("earliest_time", lambda g: g.edge("a", "b").earliest_time),
    ("latest_time", lambda g: g.edge("a", "b").latest_time),
    ("layer_names", lambda g: sorted(g.edge("a", "b").layer_names)),
    ("history", lambda g: list(g.edge("a", "b").history)),
]


@pytest.mark.parametrize("name,fn", EDGE_READS, ids=[c[0] for c in EDGE_READS])
def test_edge_read_parity(rich_pair, name, fn):
    assert_parity(rich_pair, fn)


# --- Nodes / Edges collections ---------------------------------------------

COLLECTION_READS = [
    ("node_names", lambda g: sorted(n.name for n in g.nodes)),
    ("node_types", lambda g: sorted(str(n.node_type) for n in g.nodes)),
    ("node_degrees", lambda g: sorted(n.degree() for n in g.nodes)),
    ("edge_pairs", lambda g: sorted((e.src.name, e.dst.name) for e in g.edges)),
    ("edge_count", lambda g: len(g.edges)),
    (
        "layer_edges",
        lambda g: sorted((e.src.name, e.dst.name) for e in g.layer("knows").edges),
    ),
]


@pytest.mark.parametrize(
    "name,fn", COLLECTION_READS, ids=[c[0] for c in COLLECTION_READS]
)
def test_collection_read_parity(rich_pair, name, fn):
    assert_parity(rich_pair, fn)


# --- PathFromNode (traversal) ----------------------------------------------

PATH_READS = [
    ("neighbours_names", lambda g: sorted(n.name for n in g.node("a").neighbours)),
    # `.count()` is a remote-only extra (local PathFromNode has none) — count via
    # iteration, which both sides support.
    ("neighbours_count", lambda g: sum(1 for _ in g.node("a").neighbours)),
    ("two_hop", lambda g: sorted(n.name for n in g.node("a").neighbours.neighbours)),
]


@pytest.mark.parametrize("name,fn", PATH_READS, ids=[c[0] for c in PATH_READS])
def test_path_read_parity(rich_pair, name, fn):
    assert_parity(rich_pair, fn)


# --- sorted() ----------------------------------------------------------------


def _build_sortable(g):
    g.add_node(3, "carol", properties={"score": 10}, node_type="user")
    g.add_node(1, "alice", properties={"score": 30}, node_type="admin")
    g.add_node(2, "bob", properties={"score": 20})
    g.add_edge(1, "bob", "carol", properties={"weight": 5.0})
    g.add_edge(2, "alice", "carol", properties={"weight": 1.0})
    g.add_edge(3, "alice", "bob", properties={"weight": 9.0})


def test_nodes_sorted_parity():
    """`Nodes.sorted(...)` takes the same sort-key types on both sides and
    yields the same order — single key, reversed, multi-key with tie-break,
    and a missing property falling through to the next key."""
    from raphtory import NodeSortBy, SortByTime

    key_lists = [
        [NodeSortBy.by_id()],
        [NodeSortBy.by_name(reverse=True)],
        [NodeSortBy.by_property("score")],
        [NodeSortBy.by_time(SortByTime.EARLIEST)],
        [NodeSortBy.by_type(), NodeSortBy.by_property("score", reverse=True)],
    ]
    with graph_pair(_build_sortable) as pair:
        base = [n.name for n in pair.local.nodes]
        for keys in key_lists:
            local = [n.name for n in pair.local.nodes.sorted(keys)]
            remote = [n.name for n in pair.remote.nodes.sorted(keys)]
            assert local == remote, f"sorted({keys}) diverged: {local} vs {remote}"
            # non-vacuity: every key list reorders this graph (insertion order
            # is carol, alice, bob — no key agrees with it)
            assert local != base, f"sorted({keys}) did not discriminate"

        # a key that selects nothing (missing property) compares equal and
        # falls through to the next key — on both sides
        fallthrough = [NodeSortBy.by_property("nope"), NodeSortBy.by_name()]
        by_name = [NodeSortBy.by_name()]
        local = [n.name for n in pair.local.nodes.sorted(fallthrough)]
        assert local == [n.name for n in pair.local.nodes.sorted(by_name)]
        assert local == [n.name for n in pair.remote.nodes.sorted(fallthrough)]

        # sorted() returns a live collection: view chains compose on it and
        # still agree across sides
        local = [n.name for n in pair.local.nodes.sorted(by_name).window(0, 2)]
        remote = [n.name for n in pair.remote.nodes.sorted(by_name).window(0, 2)]
        assert local == remote


def test_edges_sorted_parity():
    """`Edges.sorted(...)` agrees across sides for endpoint keys (nested
    NodeSortBy with its own reverse), time and property keys."""
    from raphtory import EdgeSortBy, NodeSortBy, SortByTime

    key_lists = [
        [
            EdgeSortBy.by_src(NodeSortBy.by_name()),
            EdgeSortBy.by_dst(NodeSortBy.by_name()),
        ],
        [EdgeSortBy.by_src(NodeSortBy.by_name(reverse=True))],
        [EdgeSortBy.by_property("weight")],
        [EdgeSortBy.by_time(SortByTime.LATEST, reverse=True)],
    ]
    with graph_pair(_build_sortable) as pair:
        for keys in key_lists:
            local = [(e.src.name, e.dst.name) for e in pair.local.edges.sorted(keys)]
            remote = [(e.src.name, e.dst.name) for e in pair.remote.edges.sorted(keys)]
            assert local == remote, f"sorted({keys}) diverged: {local} vs {remote}"
