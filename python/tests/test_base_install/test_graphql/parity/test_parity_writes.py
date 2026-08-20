"""Write-path parity: mutate both graphs identically, then read back and compare.

Each ``build`` applies the same writes to a local ``Graph`` and a ``RemoteGraph``
(node/edge adds, metadata, temporal updates, entity-scoped mutators), and the
assertions confirm the resulting state reads back identically on both sides.
"""

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
