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
    gg.add_node(1, "B")
    with pytest.raises(Exception) as excinfo:
        gg.import_nodes([g_a, g_b])
    assert "Nodes already exist" in str(excinfo.value)
    assert gg.node("A") is None

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

    gg = Graph()
    gg.add_edge(1, "C", "D")
    with pytest.raises(Exception) as excinfo:
        gg.import_edges([e_a_b, e_c_d])
    assert "Edges already exist" in str(excinfo.value)
    assert gg.edge("A", "B") is None


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


def test_import_node_as():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    gg = Graph()
    res = gg.import_node_as(a, "X")
    assert res.name == "X"
    assert res.history().tolist() == [1]

    gg.add_node(1, "Y")

    with pytest.raises(Exception) as excinfo:
        gg.import_node_as(b, "Y")

    assert "Node already exists" in str(excinfo.value)

    assert gg.nodes.name == ["X", "Y"]
    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None


def test_import_node_as_merge():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    gg = Graph()
    res = gg.import_node_as(a, "X")
    assert res.name == "X"
    assert res.history().tolist() == [1]

    gg.add_node(1, "Y")
    gg.import_node_as(b, "Y", True)

    assert gg.nodes.name == ["X", "Y"]
    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1]
    assert y.properties.get("temp") == True
    assert y.properties.constant.get("con") == 11


def test_import_nodes_as():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    gg = Graph()
    gg.add_node(1, "Y")

    with pytest.raises(Exception) as excinfo:
        gg.import_nodes_as([a, b], ["X", "Y"])

    assert "Nodes already exist" in str(excinfo.value)

    assert gg.node("X") == None

    assert sorted(gg.nodes.name) == ["Y"]
    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None


def test_import_nodes_as_merge():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    gg = Graph()
    gg.add_node(1, "Y")
    gg.import_nodes_as([a, b], ["X", "Y"], True)

    assert sorted(gg.nodes.name) == ["X", "Y"]
    x = gg.node("X")
    assert x.name == "X"
    assert x.history().tolist() == [1]

    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1]
    assert y.properties.get("temp") == True
    assert y.properties.constant.get("con") == 11


def test_import_edge_as():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    e_a_b = g.add_edge(2, "A", "B", {"e_temp": True})
    e_b_c = g.add_edge(2, "B", "C", {"e_temp": True})

    gg = Graph()
    gg.add_edge(1, "X", "Y")

    gg.import_edge_as(e_b_c, ("Y", "Z"))

    with pytest.raises(Exception) as excinfo:
        gg.import_edge_as(e_a_b, ("X", "Y"))
    assert "Edge already exists" in str(excinfo.value)

    assert sorted(gg.nodes.name) == ["X", "Y", "Z"]
    x = gg.node("X")
    assert x.name == "X"
    assert x.history().tolist() == [1]

    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1, 2]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None

    e = gg.edge("X", "Y")
    assert e.properties.get("e_temp") is None


def test_import_edge_as_merge():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})

    e_a_b = g.add_edge(2, "A", "B", {"e_temp": True})

    gg = Graph()
    gg.add_edge(3, "X", "Y")
    gg.import_edge_as(e_a_b, ("X", "Y"), True)

    assert sorted(gg.nodes.name) == ["X", "Y"]
    x = gg.node("X")
    assert x.name == "X"
    print(x.history())
    assert x.history().tolist() == [2, 3]

    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [2, 3]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None

    e = gg.edge("X", "Y")
    assert e.properties.get("e_temp") == True


def test_import_edges_as():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})
    c = g.add_node(1, "C")

    e_a_b = g.add_edge(2, "A", "B", {"e_temp": True})
    e_b_c = g.add_edge(2, "B", "C")

    gg = Graph()
    gg.add_edge(1, "Y", "Z")

    with pytest.raises(Exception) as excinfo:
        gg.import_edges_as([e_a_b, e_b_c], [("X", "Y"), ("Y", "Z")])
    assert "Edges already exist" in str(excinfo.value)

    assert sorted(gg.nodes.name) == ["Y", "Z"]

    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [1]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None

    z = gg.node("Z")
    assert z.name == "Z"
    assert z.history().tolist() == [1]


def test_import_edges_as_merge():
    g = Graph()
    a = g.add_node(1, "A")
    b = g.add_node(1, "B", {"temp": True})
    b.add_constant_properties({"con": 11})
    c = g.add_node(1, "C")

    e_a_b = g.add_edge(2, "A", "B", {"e_temp": True})
    e_b_c = g.add_edge(2, "B", "C")

    gg = Graph()
    gg.add_edge(3, "Y", "Z")
    gg.import_edges_as([e_a_b, e_b_c], [("X", "Y"), ("Y", "Z")], True)

    assert sorted(gg.nodes.name) == ["X", "Y", "Z"]

    x = gg.node("X")
    assert x.name == "X"
    assert x.history().tolist() == [2]

    y = gg.node("Y")
    assert y.name == "Y"
    assert y.history().tolist() == [2, 3]
    assert y.properties.get("temp") is None
    assert y.properties.constant.get("con") is None

    z = gg.node("Z")
    assert z.name == "Z"
    assert z.history().tolist() == [2, 3]


def test_import_edges():
    g = Graph()
    g.add_node(1, 1)
    g.add_node(1, 2)
    g.add_node(1, 3)
    g.add_edge(1, 4, 5)
    g.add_edge(1, 6, 7)
    g.add_edge(1, 8, 9)
    g2 = Graph()
    g2.import_edges(g.edges)
    assert g2.count_edges() == 3
    assert g.edges.id == g2.edges.id


def test_import_edges_iterator():
    g = Graph()
    g.add_node(1, 1)
    g.add_node(1, 2)
    g.add_node(1, 3)
    g.add_edge(1, 4, 5)
    g.add_edge(1, 6, 7)
    g.add_edge(1, 8, 9)
    g2 = Graph()
    g2.import_edges(iter(g.edges))
    assert g2.count_edges() == 3
    assert g.edges.id == g2.edges.id
