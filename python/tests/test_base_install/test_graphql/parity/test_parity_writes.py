"""Write-path parity: mutate both graphs identically, then read back and compare.

Each ``build`` applies the same writes to a local ``Graph`` and a ``RemoteGraph``
(node/edge adds, metadata, temporal updates, entity-scoped mutators), and the
assertions confirm the resulting state reads back identically on both sides.
"""

import pytest

from _parity import assert_parity, graph_pair


def test_add_nodes_and_edges_readback():
    def build(g):
        g.add_node(1, "a", node_type="person")
        g.add_node(2, "b", properties={"score": 3.0})
        g.add_edge(3, "a", "b", layer="knows")

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: sorted(n.name for n in g.nodes))
        assert_parity(pair, lambda g: g.node("a").node_type)
        assert_parity(pair, lambda g: g.node("b").properties.get("score"))
        assert_parity(pair, lambda g: sorted(g.edge("a", "b").layer_names))
        assert_parity(pair, lambda g: g.count_edges())


def test_node_metadata_readback():
    def build(g):
        g.add_node(1, "a")
        g.node("a").add_metadata({"country": "uk"})

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.node("a").metadata.get("country"))


def test_node_add_updates_readback():
    def build(g):
        g.add_node(1, "a")
        g.node("a").add_updates(5, properties={"score": 9.0})

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.node("a").properties.get("score"))
        assert_parity(pair, lambda g: g.node("a").latest_time)


def test_edge_add_updates_readback():
    def build(g):
        g.add_edge(1, "a", "b")
        g.edge("a", "b").add_updates(5, properties={"weight": 2.0})

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.edge("a", "b").properties.get("weight"))
        assert_parity(pair, lambda g: g.edge("a", "b").latest_time)


def test_same_timestamp_event_id_readback():
    """`(timestamp, event_id)` writes persist distinctly on both sides."""

    def build(g):
        g.add_edge(5, "a", "b", event_id=0)
        g.add_edge(5, "c", "d", event_id=1)

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: sorted((e.src.name, e.dst.name) for e in g.edges))
        assert_parity(pair, lambda g: g.count_edges())


# The write methods take a bare timestamp plus an optional `event_id` kwarg, so
# a `(timestamp, event_id)` tuple is not an accepted spelling of the time — and
# accepting one is worse than useless: the remote used to take the tuple and
# silently drop its event id, writing `(5, 0)` for `(5, 3)` while local raised.
# A value-comparing test cannot see that (the write *succeeded* on both sides,
# just with different data), so the refusal itself is what gets asserted.
_TUPLE_TIME_WRITES = {
    "graph.add_node": lambda g: g.add_node((5, 3), "a"),
    "graph.create_node": lambda g: g.create_node((5, 3), "z"),
    "graph.add_edge": lambda g: g.add_edge((5, 3), "a", "b"),
    "graph.delete_edge": lambda g: g.delete_edge((5, 3), "a", "b"),
    "graph.add_properties": lambda g: g.add_properties((5, 3), {"k": 1}),
    "node.add_updates": lambda g: g.node("a").add_updates((5, 3), {"k": 1}),
    "edge.add_updates": lambda g: g.edge("a", "b").add_updates((5, 3), {"k": 1}),
    "edge.delete": lambda g: g.edge("a", "b").delete((5, 3)),
}


@pytest.fixture(scope="module")
def write_pair():
    def build(g):
        g.add_node(1, "a")
        g.add_edge(1, "a", "b")

    with graph_pair(build) as pair:
        yield pair


@pytest.mark.parametrize("name", sorted(_TUPLE_TIME_WRITES))
def test_a_tuple_time_is_refused_by_both_sides(write_pair, name):
    """Both sides refuse it, with the same exception type.

    `assert_parity` asserts exception parity, so a side that accepted the tuple
    (or raised a different type) fails here.
    """
    assert_parity(write_pair, _TUPLE_TIME_WRITES[name])


@pytest.mark.parametrize("name", sorted(_TUPLE_TIME_WRITES))
def test_a_tuple_time_writes_nothing(write_pair, name):
    """Anti-vacuity: the refusal must leave the graph untouched, per side.

    Parity alone would pass if both sides refused *and* both left debris; this
    pins that a rejected write changes nothing at all.
    """
    for side in (write_pair.local, write_pair.remote):
        before = (side.count_nodes(), side.count_edges(), side.latest_time)
        with pytest.raises(Exception):
            _TUPLE_TIME_WRITES[name](side)
        assert (side.count_nodes(), side.count_edges(), side.latest_time) == before
