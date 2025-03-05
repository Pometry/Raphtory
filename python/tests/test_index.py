from raphtory import Graph, Prop
import pytest
from raphtory import filter

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


def search_nodes(graph, filter_expr, limit=20, offset=0):
    return sorted([node.name for node in graph.search_nodes(filter_expr, limit, offset)])


def test_search_nodes_for_node_name_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_name() == "N1"
    results = search_nodes(g, filter_expr)
    assert ["N1"] == results


def test_search_nodes_for_node_name_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_name() != "N1"
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


def test_search_nodes_for_node_name_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_name().includes(["N1", "N9"])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N9'] == results


def test_search_nodes_for_node_name_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_name().excludes(['N10', 'N11', 'N12', 'N13', 'N14'])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N15', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


def test_search_nodes_for_node_name_fuzzy_match():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_name().fuzzy_search('1', 1, False)
    results = search_nodes(g, filter_expr)
    assert ['N1'] == results


def test_search_nodes_for_node_type_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_type() == "fire_nation"
    results = search_nodes(g, filter_expr)
    assert ["N1", "N10", "N11", "N6"] == results


def test_search_nodes_for_node_type_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_type() != "water_tribe"
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N6', 'N7', 'N8', 'N9'] == results


def test_search_nodes_for_node_type_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_type().includes(["air_nomads", "fire_nation"])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N6', 'N7', 'N8'] == results


def test_search_nodes_for_node_type_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_type().excludes(['water_tribe', 'air_nomads', 'fire_nation'])
    results = search_nodes(g, filter_expr)
    assert ['N2', 'N9'] == results


def test_search_nodes_for_node_type_fuzzy_match():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.node_type().fuzzy_search('air', 1, False)
    results = search_nodes(g, filter_expr)
    assert ['N12', 'N13', 'N7', 'N8'] == results


def test_search_nodes_for_property_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1") == 1
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N14', 'N15', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1") != 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_lt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("q1") < 2
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N14'] == results


def test_search_nodes_for_property_le():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("q1") <= 3
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N14'] == results


def test_search_nodes_for_property_gt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1") > 2
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13'] == results


def test_search_nodes_for_property_ge():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1") >= 2
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").includes([2])
    results = search_nodes(g, filter_expr)
    assert ['N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").excludes([2])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_is_some():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").is_some()
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_nodes_for_property_is_none():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").is_none()
    results = search_nodes(g, filter_expr)
    assert [] == results


def test_search_nodes_for_property_constant_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() == 1
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N9'] == results


def test_search_nodes_for_property_constant_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() != 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N9'] == results


def test_search_nodes_for_property_constant_lt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() < 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N9'] == results


def test_search_nodes_for_property_constant_le():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() <= 3
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N4', 'N9'] == results


def test_search_nodes_for_property_constant_gt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() > 1
    results = search_nodes(g, filter_expr)
    assert ['N4'] == results


def test_search_nodes_for_property_constant_ge():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant() >= 2
    results = search_nodes(g, filter_expr)
    assert ['N4'] == results


def test_search_nodes_for_property_constant_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant().includes([2])
    results = search_nodes(g, filter_expr)
    assert ['N4'] == results


def test_search_nodes_for_property_constant_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant().excludes([2])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N9'] == results


def test_search_nodes_for_property_constant_is_some():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant().is_some()
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N4', 'N9'] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_nodes_for_property_constant_is_none():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").constant().is_none()
    results = search_nodes(g, filter_expr)
    assert [] == results


def test_search_nodes_for_property_temporal_any_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() == 1
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8'] == results


def test_search_nodes_for_property_temporal_any_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() != 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8'] == results


def test_search_nodes_for_property_temporal_any_lt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() < 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8'] == results


def test_search_nodes_for_property_temporal_any_le():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() <= 3
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_any_gt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() > 1
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_any_ge():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any() >= 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_any_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any().includes([2])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_any_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any().excludes([2])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8'] == results


def test_search_nodes_for_property_temporal_any_is_some():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any().is_some()
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_nodes_for_property_temporal_any_is_none():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().any().is_none()
    results = search_nodes(g, filter_expr)
    assert [] == results


def test_search_nodes_for_property_temporal_latest_eq():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() == 1
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_temporal_latest_ne():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() != 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_temporal_latest_lt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() < 2
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_temporal_latest_le():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() <= 3
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_latest_gt():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() > 1
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_latest_ge():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest() >= 2
    results = search_nodes(g, filter_expr)
    assert ['N10', 'N11', 'N12', 'N13', 'N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_latest_includes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest().includes([2])
    results = search_nodes(g, filter_expr)
    assert ['N2', 'N5', 'N8', 'N9'] == results


def test_search_nodes_for_property_temporal_latest_excludes():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest().excludes([2])
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N3', 'N4', 'N6', 'N7'] == results


def test_search_nodes_for_property_temporal_latest_is_some():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest().is_some()
    results = search_nodes(g, filter_expr)
    assert ['N1', 'N10', 'N11', 'N12', 'N13', 'N2', 'N3', 'N4', 'N5', 'N6', 'N7', 'N8', 'N9'] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_nodes_for_property_temporal_latest_is_none():
    g = Graph()
    g = init_graph(g)

    filter_expr = filter.Node.property("p1").temporal().latest().is_none()
    results = search_nodes(g, filter_expr)
    assert [] == results


def init_edges_graph(graph):
    """Initializes the graph with nodes and properties."""

    # Adding nodes with properties
    graph.add_edge(6, "N1", "N2", {"p1": 2}, "fire_nation")
    graph.add_edge(7, "N1", "N2", {"p1": 1}, "fire_nation")
    graph.edge("N1", "N2").add_constant_properties({"p1": 1}, layer="fire_nation")

    graph.add_edge(6, "N2", "N3", {"p1": 1}, "earth_kingdom")
    graph.add_edge(7, "N2", "N3", {"p1": 2}, "earth_kingdom")

    graph.add_edge(8, "N3", "N4", {"p1": 1}, "water_tribe")

    graph.add_edge(9, "N4", "N5", {"p1": 1}, "water_tribe")
    graph.edge("N4", "N5").add_constant_properties({"p1": 2}, layer="water_tribe")

    graph.add_edge(5, "N5", "N6", {"p1": 1}, "water_tribe")
    graph.add_edge(6, "N5", "N6", {"p1": 2}, "water_tribe")

    graph.add_edge(5, "N6", "N7", {"p1": 1}, "fire_nation")
    graph.add_edge(6, "N6", "N7", {"p1": 1}, "fire_nation")

    graph.add_edge(3, "N7", "N8", {"p1": 1}, "air_nomads")
    graph.add_edge(5, "N7", "N8", {"p1": 1}, "air_nomads")

    graph.add_edge(3, "N8", "N9", {"p1": 1}, "air_nomads")
    graph.add_edge(4, "N8", "N9", {"p1": 2}, "air_nomads")

    graph.add_edge(2, "N9", "N10", {"p1": 2}, "earth_kingdom")
    graph.edge("N9", "N10").add_constant_properties({"p1": 1}, layer="earth_kingdom")

    graph.add_edge(2, "N10", "N11", {"q1": 0}, "fire_nation")
    graph.add_edge(2, "N10", "N11", {"p1": 3}, "fire_nation")
    graph.edge("N10", "N11").add_constant_properties({"p1": 1}, layer="fire_nation")

    graph.add_edge(2, "N11", "N12", {"p1": 3}, "fire_nation")
    graph.add_edge(2, "N11", "N12", {"q1": 0}, "fire_nation")
    graph.edge("N11", "N12").add_constant_properties({"p1": 1}, layer="fire_nation")

    graph.add_edge(2, "N12", "N13", {"q1": 0}, "air_nomads")
    graph.add_edge(3, "N12", "N13", {"p1": 3}, "air_nomads")
    graph.edge("N12", "N13").add_constant_properties({"p1": 1}, layer="air_nomads")

    graph.add_edge(2, "N13", "N14", {"q1": 0}, "air_nomads")
    graph.add_edge(3, "N13", "N14", {"p1": 3}, "air_nomads")
    graph.edge("N13", "N14").add_constant_properties({"p1": 1}, layer="air_nomads")

    graph.add_edge(2, "N14", "N15", {"q1": 0}, "water_tribe")
    graph.edge("N14", "N15").add_constant_properties({"p1": 1}, layer="water_tribe")

    graph.add_edge(2, "N15", "N1", {}, "water_tribe")  # NO_PROPS equivalent
    graph.edge("N15", "N1").add_constant_properties({"p1": 1}, layer="water_tribe")

    return graph


def search_edges(graph, filter_expr, limit=20, offset=0):
    return sorted([(edge.src.name, edge.dst.name) for edge in graph.search_edges(filter_expr, limit, offset)])


def test_search_edges_for_src_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.src() == "N1"
    results = search_edges(g, filter_expr)
    assert [("N1", "N2")] == results


def test_search_edges_for_src_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.src() != "N1"
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_src_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.src().includes(["N1", "N9"])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N9', 'N10')] == results


def test_search_edges_for_src_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.src().excludes(['N10', 'N11', 'N12', 'N13', 'N14'])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N15', 'N1'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_src_fuzzy_match():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.src().fuzzy_search('1', 1, False)
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2')] == results


def test_search_edges_for_dst_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.dst() == "N1"
    results = search_edges(g, filter_expr)
    assert [('N15', 'N1')] == results


def test_search_edges_for_dst_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.dst() != "N1"
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_dst_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.dst().includes(["N1", "N9"])
    results = search_edges(g, filter_expr)
    assert [('N15', 'N1'), ('N8', 'N9')] == results


def test_search_edges_for_dst_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.dst().excludes(['N1', 'N9', 'N10'])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_dst_fuzzy_match():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.dst().fuzzy_search('1', 1, False)
    results = search_edges(g, filter_expr)
    assert [('N15', 'N1')] == results


def test_search_edges_for_property_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1") == 1
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N14', 'N15'), ('N15', 'N1'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1") != 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_lt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("q1") < 2
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15')]== results


def test_search_edges_for_property_le():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("q1") <= 3
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15')] == results


def test_search_edges_for_property_gt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1") > 2
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14')] == results


def test_search_edges_for_property_ge():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1") >= 2
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").includes([2])
    results = search_edges(g, filter_expr)
    assert [('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").excludes([2])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_is_some():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").is_some()
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_edges_for_property_is_none():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").is_none()
    results = search_edges(g, filter_expr)
    assert [] == results


def test_search_edges_for_property_constant_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() == 1
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N9', 'N10')] == results


def test_search_edges_for_property_constant_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() != 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N9', 'N10')] == results


def test_search_edges_for_property_constant_lt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() < 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N9', 'N10')] == results


def test_search_edges_for_property_constant_le():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() <= 3
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N4', 'N5'), ('N9', 'N10')] == results


def test_search_edges_for_property_constant_gt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() > 1
    results = search_edges(g, filter_expr)
    assert [('N4', 'N5')] == results


def test_search_edges_for_property_constant_ge():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant() >= 2
    results = search_edges(g, filter_expr)
    assert [('N4', 'N5')] == results


def test_search_edges_for_property_constant_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant().includes([2])
    results = search_edges(g, filter_expr)
    assert [('N4', 'N5')] == results


def test_search_edges_for_property_constant_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant().excludes([2])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N9', 'N10')] == results


def test_search_edges_for_property_constant_is_some():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant().is_some()
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N14', 'N15'), ('N15', 'N1'), ('N4', 'N5'), ('N9', 'N10')] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_edges_for_property_constant_is_none():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").constant().is_none()
    results = search_edges(g, filter_expr)
    assert [] == results


def test_search_edges_for_property_temporal_any_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() == 1
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9')] == results


def test_search_edges_for_property_temporal_any_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() != 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9')] == results


def test_search_edges_for_property_temporal_any_lt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() < 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9')] == results


def test_search_edges_for_property_temporal_any_le():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() <= 3
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_any_gt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() > 1
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_any_ge():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any() >= 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_any_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any().includes([2])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_any_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any().excludes([2])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9')] == results


def test_search_edges_for_property_temporal_any_is_some():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any().is_some()
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_edges_for_property_temporal_any_is_none():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().any().is_none()
    results = search_edges(g, filter_expr)
    assert [] == results


def test_search_edges_for_property_temporal_latest_eq():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() == 1
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_temporal_latest_ne():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() != 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_temporal_latest_lt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() < 2
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_temporal_latest_le():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() <= 3
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_latest_gt():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() > 1
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_latest_ge():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest() >= 2
    results = search_edges(g, filter_expr)
    assert [('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_latest_includes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest().includes([2])
    results = search_edges(g, filter_expr)
    assert  [('N2', 'N3'), ('N5', 'N6'), ('N8', 'N9'), ('N9', 'N10')] == results


def test_search_edges_for_property_temporal_latest_excludes():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest().excludes([2])
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N3', 'N4'), ('N4', 'N5'), ('N6', 'N7'), ('N7', 'N8')] == results


def test_search_edges_for_property_temporal_latest_is_some():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest().is_some()
    results = search_edges(g, filter_expr)
    assert [('N1', 'N2'), ('N10', 'N11'), ('N11', 'N12'), ('N12', 'N13'), ('N13', 'N14'), ('N2', 'N3'), ('N3', 'N4'), ('N4', 'N5'), ('N5', 'N6'), ('N6', 'N7'), ('N7', 'N8'), ('N8', 'N9'), ('N9', 'N10')] == results


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_search_edges_for_property_temporal_latest_is_none():
    g = Graph()
    g = init_edges_graph(g)

    filter_expr = filter.Edge.property("p1").temporal().latest().is_none()
    results = search_edges(g, filter_expr)
    assert [] == results
    