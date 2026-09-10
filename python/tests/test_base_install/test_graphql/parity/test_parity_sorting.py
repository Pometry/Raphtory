"""Ordering parity for the remote-only `sorted(...)` surface, incl. nested keys.

The local ``Nodes``/``Edges`` have **no** ``sorted()`` — it is a server-side
extra (the ceiling, not the drop-in floor), so there is nothing to diff against
directly. Rather than skip it, the expected order is *derived from the local
graph*: the same key is applied with Python's (stable) ``sorted`` over the local
collection, and the remote's ordering must reproduce it. The local ``Graph``
stays the oracle; only the sort itself is remote.

This is the same convention already used for other remote-only extras (e.g.
``PathFromNode.count()`` in ``test_parity_entities``): expressed against the
local side rather than ledgered as a gap, because a gap entry means "local has
it, remote does not" — here it is the other way round.
"""

import pytest
from raphtory import EdgeSortBy, NodeSortBy

from _parity import graph_pair


def _build_sortable(g):
    """Node names and node types deliberately disagree on ordering.

    Alphabetical by name is a,b,c,d; alphabetical by type is ant(b), kiwi(d),
    mole(c), zebra(a) — so a by-name result cannot accidentally satisfy a
    by-type assertion.
    """
    g.add_node(1, "a", node_type="zebra")
    g.add_node(1, "b", node_type="ant")
    g.add_node(1, "c", node_type="mole")
    g.add_node(1, "d", node_type="kiwi")
    g.add_edge(2, "b", "a")
    g.add_edge(3, "a", "c")
    g.add_edge(4, "c", "b")
    g.add_edge(5, "a", "b")
    g.add_edge(6, "d", "c")


@pytest.fixture(scope="module")
def sortable_pair():
    with graph_pair(_build_sortable) as pair:
        yield pair


def _pairs(edges):
    return [(e.src.name, e.dst.name) for e in edges]


def _expected(pair, key, reverse=False):
    """Stable-sort the *local* edges by ``key`` — the expected remote order.

    Ties fall back to the collection's natural order, which is asserted equal
    on both sides by ``test_unsorted_order_matches`` below, so a stable sort on
    the local side predicts the remote one.
    """
    return _pairs(sorted(pair.local.edges, key=key, reverse=reverse))


def test_unsorted_order_matches(sortable_pair):
    """Precondition for the stable-sort oracle: pre-sort order agrees."""
    assert _pairs(sortable_pair.local.edges) == _pairs(sortable_pair.remote.edges)


# --- nested node keys under an edge key -------------------------------------


def test_sorted_by_src_name(sortable_pair):
    got = _pairs(
        sortable_pair.remote.edges.sorted([EdgeSortBy.by_src(NodeSortBy.by_name())])
    )
    assert got == _expected(sortable_pair, key=lambda e: e.src.name)


def test_sorted_by_src_name_reverse(sortable_pair):
    """`reverse` lives on the *nested* node key, not the edge key."""
    got = _pairs(
        sortable_pair.remote.edges.sorted([EdgeSortBy.by_src(NodeSortBy.by_name(True))])
    )
    assert got == _expected(sortable_pair, key=lambda e: e.src.name, reverse=True)


def test_sorted_by_src_type(sortable_pair):
    got = _pairs(
        sortable_pair.remote.edges.sorted([EdgeSortBy.by_src(NodeSortBy.by_type())])
    )
    assert got == _expected(sortable_pair, key=lambda e: str(e.src.node_type))


def test_sorted_by_dst_name(sortable_pair):
    got = _pairs(
        sortable_pair.remote.edges.sorted([EdgeSortBy.by_dst(NodeSortBy.by_name())])
    )
    assert got == _expected(sortable_pair, key=lambda e: e.dst.name)


def test_sorted_by_neighbour_name(sortable_pair):
    """For a graph-level edge collection the neighbour is the destination."""
    got = _pairs(
        sortable_pair.remote.edges.sorted(
            [EdgeSortBy.by_neighbour(NodeSortBy.by_name())]
        )
    )
    assert got == _expected(sortable_pair, key=lambda e: e.dst.name)


def test_sorted_by_neighbour_type(sortable_pair):
    got = _pairs(
        sortable_pair.remote.edges.sorted(
            [EdgeSortBy.by_neighbour(NodeSortBy.by_type())]
        )
    )
    assert got == _expected(sortable_pair, key=lambda e: str(e.dst.node_type))


# --- multi-key (lexicographic) ----------------------------------------------


def test_sorted_multi_key_src_then_dst(sortable_pair):
    """Ties on the first key break to the second — a total order here."""
    got = _pairs(
        sortable_pair.remote.edges.sorted(
            [
                EdgeSortBy.by_src(NodeSortBy.by_name()),
                EdgeSortBy.by_dst(NodeSortBy.by_name()),
            ]
        )
    )
    assert got == _expected(sortable_pair, key=lambda e: (e.src.name, e.dst.name))


def test_sorted_multi_key_srctype_then_dstname(sortable_pair):
    got = _pairs(
        sortable_pair.remote.edges.sorted(
            [
                EdgeSortBy.by_src(NodeSortBy.by_type()),
                EdgeSortBy.by_dst(NodeSortBy.by_name()),
            ]
        )
    )
    assert got == _expected(
        sortable_pair, key=lambda e: (str(e.src.node_type), e.dst.name)
    )


def test_sorted_preserves_membership(sortable_pair):
    """Sorting reorders, it must not add or drop edges."""
    sorted_edges = sortable_pair.remote.edges.sorted(
        [EdgeSortBy.by_src(NodeSortBy.by_name())]
    )
    assert sorted(_pairs(sorted_edges)) == sorted(_pairs(sortable_pair.local.edges))


# --- node collection keys ---------------------------------------------------


def test_nodes_sorted_by_name(sortable_pair):
    got = [n.name for n in sortable_pair.remote.nodes.sorted([NodeSortBy.by_name()])]
    assert got == sorted(n.name for n in sortable_pair.local.nodes)


def test_nodes_sorted_by_type(sortable_pair):
    got = [n.name for n in sortable_pair.remote.nodes.sorted([NodeSortBy.by_type()])]
    expected = [
        n.name
        for n in sorted(sortable_pair.local.nodes, key=lambda n: str(n.node_type))
    ]
    assert got == expected
