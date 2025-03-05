from raphtory import Graph
import pytest
from raphtory import filter

# Range queries are not supported for properties in tantivy (as of 0.22) because they are implemented as json.
# See https://github.com/quickwit-oss/tantivy/issues/1709

@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_in_python():
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2,
        "ben",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_node(
        3,
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )

    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        1,
        "ben",
        "hamza",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_edge(
        3,
        "ben",
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )

    # Name tests
    assert len(g.search_nodes("name:ben")) == 1
    assert len(g.search_nodes("name:ben OR name:hamza")) == 2
    assert len(g.search_nodes("name:ben AND name:hamza")) == 0
    assert len(g.search_nodes("name: IN [ben, hamza]")) == 2

    # Property tests
    # assert len(g.search_nodes("value:<120 OR value_f:>30")) == 3
    assert len(g.search_nodes("value:199")) == 1
    # assert len(g.search_nodes("value:[0 TO 60]")) == 2
    # assert len(g.search_nodes("value:[0 TO 60}")) == 1  # } == exclusive
    # assert len(g.search_nodes("value:>59 AND value_str:abc123")) == 1

    # edge tests
    assert len(g.search_edges("from:ben")) == 2
    assert len(g.search_edges("from:ben OR from:haaroon")) == 3
    assert len(g.search_edges("to:haaroon AND from:ben")) == 1
    assert len(g.search_edges("to: IN [ben, hamza]")) == 2

    # edge prop tests
    # assert len(g.search_edges("value:<120 OR value_f:>30")) == 3
    # assert len(g.search_edges("value:[0 TO 60]")) == 2
    # assert len(g.search_edges("value:[0 TO 60}")) == 1  # } == exclusive
    # assert len(g.search_edges("value:>59 AND value_str:abc123")) == 1

    # Multiple history points test
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2, "hamza", properties={"value": 70, "value_f": 21.3, "value_str": "avc125"}
    )
    g.add_node(
        3, "hamza", properties={"value": 80, "value_f": 11.3, "value_str": "dsc2312"}
    )

    # The semantics here are that the expressions independently need to evaluate at ANY point in the lifetime of the node - hence hamza is returned even though at no point does he have both these values at the same time
    # assert len(g.search_nodes("value:<70 AND value_f:<19.2")) == 1

    g.add_node(
        4, "hamza", properties={"value": 100, "value_f": 11.3, "value_str": "dsc2312"}
    )
    # the graph isn't currently reindexed so this will not return hamza even though he now has a value which fits the bill
    # assert len(g.search_nodes("value:>99")) == 0

@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_type_search():
    g = Graph()
    ben = g.add_node(1, "ben", node_type="type_1")
    hamza = g.add_node(2, "hamza", node_type="type_2")
    assert g.search_nodes("node_type:type_1") == [ben]
    assert set(g.search_nodes("node_type:type_1 OR node_type:type_2")) == {
        hamza,
        ben,
    }

@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_with_windows():
    # Window test
    g = Graph()
    g.add_node(
        1, "hamza", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    g.add_node(
        2, "hamza", properties={"value": 70, "value_f": 21.3, "value_str": "avc125"}
    )
    g.add_node(
        3, "hamza", properties={"value": 80, "value_f": 11.3, "value_str": "dsc2312"}
    )

    g.add_edge(
        1,
        "haaroon",
        "hamza",
        properties={"value": 50, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 21.3, "value_str": "abddasc1223"},
    )
    g.add_edge(
        3,
        "haaroon",
        "hamza",
        properties={"value": 70, "value_f": 11.3, "value_str": "abdsda2c123"},
    )
    g.add_edge(
        4,
        "ben",
        "naomi",
        properties={"value": 100, "value_f": 22.3, "value_str": "ddddd"},
    )

    w_g = g.window(1, 3)


    # Testing if windowing works - ben shouldn't be included and Hamza should only have max value of 70
    assert len(w_g.search_nodes("name:ben")) == 0
    assert len(w_g.search_nodes("value:70")) == 1
    # assert len(w_index.search_nodes("value:>80")) == 0

    assert len(w_g.search_edges("from:ben")) == 0
    # assert len(w_index.search_edges("from:haaroon AND value:>70")) == 0
    assert len(w_g.search_edges("from:haaroon AND to:hamza")) == 1

@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_with_subgraphs():
    g = Graph()
    g.add_edge(
        2,
        "haaroon",
        "hamza",
        properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
    )
    g.add_edge(
        1,
        "ben",
        "hamza",
        properties={"value": 59, "value_f": 11.4, "value_str": "test test test"},
    )
    g.add_edge(
        3,
        "ben",
        "haaroon",
        properties={
            "value": 199,
            "value_f": 52.6,
            "value_str": "I wanna rock right now",
        },
    )
    g.add_edge(4, "hamza", "naomi")

    assert len(g.search_edges("from:hamza OR to:hamza")) == 3

    subgraph = g.subgraph([g.node("ben"), g.node("hamza"), g.node("haaroon")])

    assert len(subgraph.search_edges("from:hamza OR to:hamza")) == 2

def init_graph(graph):
    """Initializes the graph with nodes and properties."""

    # Adding nodes with properties
    graph.add_node(6, "N1", {"p1": 2}, "fire_nation")
    graph.add_node(7, "N1", {"p1": 1}, "fire_nation")
    graph.node("N1").add_constant_properties({"p1": 1})

    graph.add_node(6, "N2", {"p1": 1}, "earth_kingdom")
    graph.add_node(7, "N2", {"p1": 2}, "earth_kingdom")

    graph.add_node(8, "N3", {"p1": 1}, "water_tribe")

    graph.add_node(9, "N4", {"p1": 1}, "water_tribe")
    graph.node("N4").add_constant_properties({"p1": 2})

    graph.add_node(5, "N5", {"p1": 1}, "water_tribe")
    graph.add_node(6, "N5", {"p1": 2}, "water_tribe")

    graph.add_node(5, "N6", {"p1": 1}, "fire_nation")
    graph.add_node(6, "N6", {"p1": 1}, "fire_nation")

    graph.add_node(3, "N7", {"p1": 1}, "air_nomads")
    graph.add_node(5, "N7", {"p1": 1}, "air_nomads")

    graph.add_node(3, "N8", {"p1": 1}, "air_nomads")
    graph.add_node(4, "N8", {"p1": 2}, "air_nomads")

    graph.add_node(2, "N9", {"p1": 2}, "earth_kingdom")
    graph.node("N9").add_constant_properties({"p1": 1})

    graph.add_node(2, "N10", {"q1": 0}, "fire_nation")
    graph.add_node(2, "N10", {"p1": 3}, "fire_nation")
    graph.node("N10").add_constant_properties({"p1": 1})

    graph.add_node(2, "N11", {"p1": 3}, "fire_nation")
    graph.add_node(2, "N11", {"q1": 0}, "fire_nation")
    graph.node("N11").add_constant_properties({"p1": 1})

    graph.add_node(2, "N12", {"q1": 0}, "air_nomads")
    graph.add_node(3, "N12", {"p1": 3}, "air_nomads")
    graph.node("N12").add_constant_properties({"p1": 1})

    graph.add_node(2, "N13", {"q1": 0}, "air_nomads")
    graph.add_node(3, "N13", {"p1": 3}, "air_nomads")
    graph.node("N13").add_constant_properties({"p1": 1})

    graph.add_node(2, "N14", {"q1": 0}, "water_tribe")
    graph.node("N14").add_constant_properties({"p1": 1})

    graph.add_node(2, "N15", {}, "water_tribe")  # NO_PROPS equivalent
    graph.node("N15").add_constant_properties({"p1": 1})

    return graph


def test_search_nodes_for_node_name_eq():
    g = Graph()
    g = init_graph(g)

    results = [node.name for node in g.search_nodes(filter.Node.node_name() == "N1", 10, 0)]
    assert ["N1"] == results


def test_search_nodes_for_node_name_ne():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_name() != "N1", 10, 0)])
    assert ['N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results

def test_search_nodes_for_node_name_includes():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_name().includes(["N1", "N9"]), 10, 0)])
    assert ['N1', 'N9'] == results

def test_search_nodes_for_node_name_excludes():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_name().excludes(['N10', 'N11', 'N12', 'N13', 'N14']), 10, 0)])
    assert ['N1', 'N15', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results

def test_search_nodes_for_node_name_fuzzy_match():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_name().fuzzy_search('1', 1, False), 10, 0)])
    assert ['N1'] == results

def test_search_nodes_for_node_type_eq():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_type() == "fire_nation", 10, 0)])
    assert ["N1", "N10", "N11", "N6"] == results


def test_search_nodes_for_node_type_ne():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_type() != "water_tribe", 10, 0)])
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N6', 'N7', 'N8', 'N9'] == results

def test_search_nodes_for_node_type_includes():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_type().includes(["air_nomads", "fire_nation"]), 10, 0)])
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N6', 'N7', 'N8'] == results

def test_search_nodes_for_node_type_excludes():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_type().excludes(['water_tribe', 'air_nomads', 'fire_nation']), 10, 0)])
    assert ['N2', 'N9'] == results

def test_search_nodes_for_node_type_fuzzy_match():
    g = Graph()
    g = init_graph(g)

    results = sorted([node.name for node in g.search_nodes(filter.Node.node_type().fuzzy_search('air', 1, False), 10, 0)])
    assert ['N12', 'N13', 'N7', 'N8'] == results


def init_edges_graph(graph):
    """Initializes the graph with nodes and properties."""

    # Adding nodes with properties
    graph.add_edge(6, "N1", "N2", {"p1": 2}, "fire_nation")
    graph.add_edge(7, "N1", "N2", {"p1": 1}, "fire_nation")

    graph.add_edge(6, "N2", "N3", {"p1": 1}, "earth_kingdom")
    graph.add_edge(7, "N2", "N3", {"p1": 2}, "earth_kingdom")

    graph.add_edge(8, "N3", "N4", {"p1": 1}, "water_tribe")

    graph.add_edge(9, "N4", "N5", {"p1": 1}, "water_tribe")

    graph.add_edge(5, "N5", "N6", {"p1": 1}, "water_tribe")
    graph.add_edge(6, "N5", "N6", {"p1": 2}, "water_tribe")

    graph.add_edge(5, "N6", "N7", {"p1": 1}, "fire_nation")
    graph.add_edge(6, "N6", "N7", {"p1": 1}, "fire_nation")

    graph.add_edge(3, "N7", "N8", {"p1": 1}, "air_nomads")
    graph.add_edge(5, "N7", "N8", {"p1": 1}, "air_nomads")

    graph.add_edge(3, "N8", "N9", {"p1": 1}, "air_nomads")
    graph.add_edge(4, "N8", "N9", {"p1": 2}, "air_nomads")

    graph.add_edge(2, "N9", "N10", {"p1": 2}, "earth_kingdom")

    graph.add_edge(2, "N10", "N11", {"q1": 0}, "fire_nation")
    graph.add_edge(2, "N10", "N11", {"p1": 3}, "fire_nation")

    graph.add_edge(2, "N11", "N12", {"p1": 3}, "fire_nation")
    graph.add_edge(2, "N11", "N12", {"q1": 0}, "fire_nation")

    graph.add_edge(2, "N12", "N13", {"q1": 0}, "air_nomads")
    graph.add_edge(3, "N12", "N13", {"p1": 3}, "air_nomads")

    graph.add_edge(2, "N13", "N14", {"q1": 0}, "air_nomads")
    graph.add_edge(3, "N13", "N14", {"p1": 3}, "air_nomads")

    graph.add_edge(2, "N14", "N15", {"q1": 0}, "water_tribe")

    graph.add_edge(2, "N15", "N1", {}, "water_tribe")  # NO_PROPS equivalent

    return graph

def test_search_edges_for_src_eq():
    g = Graph()
    g = init_edges_graph(g)

    results = [(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.src() == "N1", 10, 0)]
    assert [("N1", "N2")] == results


def test_search_edges_for_src_ne():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.src() != "N1", 10, 0)])
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results

def test_search_edges_for_src_includes():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.src().includes(["N1", "N9"]), 10, 0)])
    assert [('N1', 'N2'), ('N9', 'N10')] == results

def test_search_edges_for_src_excludes():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.src().excludes(['N10', 'N11', 'N12', 'N13', 'N14']), 10, 0)])
    assert [('N1', 'N2'), ('N15', 'N1'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results

def test_search_edges_for_src_fuzzy_match():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.src().fuzzy_search('1', 1, False), 10, 0)])
    assert [('N1', 'N2')] == results

def test_search_edges_for_dst_eq():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.dst() == "N1", 10, 0)])
    assert [('N15', 'N1')] == results


def test_search_edges_for_dst_ne():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.dst() != "N1", 10, 0)])
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results

def test_search_edges_for_dst_includes():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.dst().includes(["N1", "N9"]), 10, 0)])
    assert [('N15', 'N1'), ('N8', 'N9')] == results

def test_search_edges_for_dst_excludes():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.dst().excludes(['N1', 'N9', 'N10']), 10, 0)])
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8')] == results

def test_search_edges_for_dst_fuzzy_match():
    g = Graph()
    g = init_edges_graph(g)

    results = sorted([(edge.src.name, edge.dst.name) for edge in g.search_edges(filter.Edge.dst().fuzzy_search('1', 1, False), 10, 0)])
    assert [('N15', 'N1')] == results
