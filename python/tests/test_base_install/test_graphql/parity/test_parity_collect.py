"""`collect()` view-faithfulness parity: materialized handles keep their view.

``collect()`` turns a lazy collection into concrete handles. The risk on the
remote side is that those handles come back rebased on the *bare* graph — the
window, filter or layer that produced the collection silently dropped — so a
terminal read on a collected node answers for the whole graph. Locally that
cannot happen; the handle carries its view. Each case therefore materializes
under a narrowed view on both sides and reads a terminal off the collected
handles, which must agree.

Every assertion also compares against the *unrestricted* read where the two
differ, so a test cannot pass by both sides ignoring the view.
"""

import pytest
from raphtory import filter as rfilter

from _parity import assert_parity, graph_pair


def _build_views(g):
    """Late edges (t=9) and a low-score node exist only outside the views used."""
    g.add_node(1, "a", node_type="zebra", properties={"score": 3.0})
    g.add_node(1, "b", node_type="ant", properties={"score": 1.0})
    g.add_node(1, "c", node_type="mole", properties={"score": 2.0})
    g.add_edge(2, "b", "a")
    g.add_edge(3, "a", "c")
    g.add_edge(4, "c", "b")
    g.add_edge(9, "a", "b")


@pytest.fixture(scope="module")
def view_pair():
    with graph_pair(_build_views) as pair:
        yield pair


# --- windowed collections ---------------------------------------------------

WINDOWED_READS = [
    (
        "node_names",
        lambda g: sorted(n.name for n in g.window(0, 4).nodes.collect()),
    ),
    (
        "node_latest_time",
        lambda g: sorted(
            (n.name, n.latest_time.t) for n in g.window(0, 4).nodes.collect()
        ),
    ),
    (
        "node_degree",
        lambda g: sorted((n.name, n.degree()) for n in g.window(0, 4).nodes.collect()),
    ),
    (
        "node_history",
        lambda g: sorted(
            (n.name, tuple(t.t for t in n.history))
            for n in g.window(0, 4).nodes.collect()
        ),
    ),
    (
        "edge_pairs",
        lambda g: sorted(
            (e.src.name, e.dst.name) for e in g.window(0, 4).edges.collect()
        ),
    ),
    (
        "edge_history",
        lambda g: sorted(
            (e.src.name, e.dst.name, tuple(t.t for t in e.history))
            for e in g.window(0, 4).edges.collect()
        ),
    ),
    (
        "neighbours_of_collected",
        lambda g: sorted(
            (n.name, tuple(sorted(x.name for x in n.neighbours)))
            for n in g.window(0, 4).nodes.collect()
        ),
    ),
]


@pytest.mark.parametrize("name,fn", WINDOWED_READS, ids=[c[0] for c in WINDOWED_READS])
def test_windowed_collect_parity(view_pair, name, fn):
    assert_parity(view_pair, fn)


def test_windowed_collect_actually_narrows(view_pair):
    """The window must change the answer — otherwise the parity above is vacuous.

    Node `b`'s edge from `c` lands at t=4 (outside the half-open window), so
    under `window(0, 4)` its degree is strictly smaller than unwindowed.
    """
    for side in (view_pair.local, view_pair.remote):
        collected = {n.name: n for n in side.window(0, 4).nodes.collect()}
        assert collected["b"].degree() == 1
        assert side.node("b").degree() == 2


# --- filtered collections ---------------------------------------------------

FILTERED_READS = [
    (
        "graph_filter_node_names",
        lambda g: sorted(
            n.name
            for n in g.filter(rfilter.Node.property("score") > 1.5).nodes.collect()
        ),
    ),
    (
        "graph_filter_degree",
        lambda g: sorted(
            (n.name, n.degree())
            for n in g.filter(rfilter.Node.property("score") > 1.5).nodes.collect()
        ),
    ),
    (
        "graph_filter_neighbours",
        lambda g: sorted(
            (n.name, tuple(sorted(x.name for x in n.neighbours)))
            for n in g.filter(rfilter.Node.property("score") > 1.5).nodes.collect()
        ),
    ),
    (
        "graph_filter_edges",
        lambda g: sorted(
            (e.src.name, e.dst.name)
            for e in g.filter(rfilter.Node.property("score") > 1.5).edges.collect()
        ),
    ),
    (
        "type_filter_names",
        lambda g: sorted(n.name for n in g.nodes.type_filter(["ant"]).collect()),
    ),
    # `nodes.filter()` is sticky: it keeps every member but propagates to their
    # traversals, so the collected handles' neighbours are the narrowed set.
    (
        "sticky_node_filter_neighbours",
        lambda g: sorted(
            (n.name, tuple(sorted(x.name for x in n.neighbours)))
            for n in g.nodes.filter(rfilter.Node.property("score") > 1.5).collect()
        ),
    ),
]


@pytest.mark.parametrize("name,fn", FILTERED_READS, ids=[c[0] for c in FILTERED_READS])
def test_filtered_collect_parity(view_pair, name, fn):
    assert_parity(view_pair, fn)


def test_filtered_collect_actually_narrows(view_pair):
    """Node `b` (score 1.0) is filtered out, so `a`'s only neighbour is `c`."""
    for side in (view_pair.local, view_pair.remote):
        collected = {
            n.name: n
            for n in side.filter(rfilter.Node.property("score") > 1.5).nodes.collect()
        }
        assert sorted(collected) == ["a", "c"]
        assert sorted(x.name for x in collected["a"].neighbours) == ["c"]
        assert sorted(x.name for x in side.node("a").neighbours) == ["b", "c"]


# --- composed window + filter ----------------------------------------------


def test_windowed_and_filtered_collect_parity(view_pair):
    def fn(g):
        view = g.window(0, 4).filter(rfilter.Node.property("score") > 1.5)
        return sorted(
            (n.name, n.degree(), tuple(sorted(x.name for x in n.neighbours)))
            for n in view.nodes.collect()
        )

    assert_parity(view_pair, fn)
