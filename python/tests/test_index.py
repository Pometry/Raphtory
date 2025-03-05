from raphtory import Graph
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
