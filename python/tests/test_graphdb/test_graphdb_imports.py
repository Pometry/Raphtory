import pytest
from raphtory import Graph, PersistentGraph


def test_import_into_graph():
    g = Graph()
    g_a = g.add_node(0, "A")
    g_b = g.add_node(1, "B", {"temp": True})
    g_b.add_constant_properties({"con": 11})

    gg = Graph()
    res = gg.import_node(g_a)
    assert res.name == g_a.name
    assert res.history() == g_a.history()

    res = gg.import_node(g_b)
    assert res.name == g_b.name
    assert res.history() == g_b.history()
    assert res.properties.get("temp") == True
    assert res.properties.constant.get("con") == 11

    gg = Graph()
    gg.import_nodes([g_a, g_b])
    assert len(gg.nodes) == 2
    assert [x.name for x in gg.nodes] == ["A", "B"]

    e_a_b = g.add_edge(2, "A", "B")
    res = gg.import_edge(e_a_b)
    assert (res.src.name, res.dst.name) == ("A", "B")

    props = {"etemp": False}
    e_a_b_p = g.add_edge(3, "A", "B", props)
    gg = Graph()
    res = gg.import_edge(e_a_b_p)
    assert res.properties.as_dict() == props

    e_c_d = g.add_edge(4, "C", "D")
    gg = Graph()
    gg.import_edges([e_a_b, e_c_d])
    assert len(gg.nodes) == 4
    assert len(gg.edges) == 2


def test_import_with_int():
    g = Graph()
    g.add_node(1, 1)
    g.add_node(1, 2)
    g.add_node(1, 3)
    g.add_edge(1, 4, 5)
    g.add_edge(1, 6, 7)
    g.add_edge(1, 8, 9)
    g2 = Graph()
    g2.import_edge(g.edge(4, 5))
    g2.import_edges([g.edge(6, 7), g.edge(8, 9)])
    assert g2.count_edges() == 3
    g2.import_node(g.node(1))
    g2.import_nodes([g.node(2), g.node(3)])
    assert g2.count_nodes() == g.count_nodes()
