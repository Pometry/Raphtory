from raphtory import Graph, PersistentGraph
import tempfile
import pytest
import shutil
import atexit


def combined(initializers):
    def func(graph):
        for initializer in initializers:
            graph = initializer(graph)
        return graph

    return func


def init_graph(graph):
    nodes = [
        (
            1,
            1,
            {"p1": "shivam_kapoor", "p9": 5, "p10": "Paper_airplane"},
            "fire_nation",
        ),
        (2, 2, {"p1": "prop12", "p2": 2, "p10": "Paper_ship"}, "air_nomads"),
        (3, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
        (3, 3, {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, "fire_nation"),
        (4, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
        (3, 4, {"p4": "pometry"}, None),
        (4, 4, {"p5": 12}, None),
    ]

    for time, id, props, node_type in nodes:
        graph.add_node(time, str(id), props, node_type)

    edge_data = [
        (1, "1", "2", {"p1": "shivam_kapoor", "p10": "Paper_airplane"}, "fire_nation"),
        (2, "1", "2", {"p1": "shivam_kapoor", "p2": 4}, "fire_nation"),
        (2, "2", "3", {"p1": "prop12", "p2": 2, "p10": "Paper_ship"}, "air_nomads"),
        (3, "3", "1", {"p2": 6, "p3": 1}, "fire_nation"),
        (3, "2", "1", {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, None),
        (4, "David Gilmour", "John Mayer", {"p2": 6, "p3": 1}, None),
        (4, "John Mayer", "Jimmy Page", {"p2": 6, "p3": 1}, None),
    ]

    for time, src, dst, props, edge_type in edge_data:
        graph.add_edge(time, src, dst, props, edge_type)

    return graph


def init_nodes_graph(graph):
    nodes = [
        (6, "N1", {"p1": 2}),
        (7, "N1", {"p1": 1}),
        (6, "N2", {"p1": 1}),
        (7, "N2", {"p1": 2}),
        (8, "N3", {"p1": 1}),
        (9, "N4", {"p1": 1}),
        (5, "N5", {"p1": 1}),
        (6, "N5", {"p1": 2}),
        (5, "N6", {"p1": 1}),
        (6, "N6", {"p1": 1}),
        (3, "N7", {"p1": 1}),
        (5, "N7", {"p1": 1}),
        (3, "N8", {"p1": 1}),
        (4, "N8", {"p1": 2}),
        (2, "N9", {"p1": 2}),
        (2, "N10", {"q1": 0}),
        (2, "N10", {"p1": 3}),
        (2, "N11", {"p1": 3}),
        (2, "N11", {"q1": 0}),
        (2, "N12", {"q1": 0}),
        (3, "N12", {"p1": 3}),
        (2, "N13", {"q1": 0}),
        (3, "N13", {"p1": 3}),
        (2, "N14", {"q1": 0}),
        (2, "N15", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p1": 1},
        "N4": {"p1": 2},
        "N9": {"p1": 1},
        "N10": {"p1": 1},
        "N11": {"p1": 1},
        "N12": {"p1": 1},
        "N13": {"p1": 1},
        "N14": {"p1": 1},
        "N15": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    edges = [
        (6, "N1", "N2", {"p1": 2}),
        (7, "N1", "N2", {"p1": 1}),
        (6, "N2", "N3", {"p1": 1}),
        (7, "N2", "N3", {"p1": 2}),
        (8, "N3", "N4", {"p1": 1}),
        (9, "N4", "N5", {"p1": 1}),
        (5, "N5", "N6", {"p1": 1}),
        (6, "N5", "N6", {"p1": 2}),
        (5, "N6", "N7", {"p1": 1}),
        (6, "N6", "N7", {"p1": 1}),
        (3, "N7", "N8", {"p1": 1}),
        (5, "N7", "N8", {"p1": 1}),
        (3, "N8", "N9", {"p1": 1}),
        (4, "N8", "N9", {"p1": 2}),
        (2, "N9", "N10", {"p1": 2}),
        (2, "N10", "N11", {"q1": 0}),
        (2, "N10", "N11", {"p1": 3}),
        (2, "N11", "N12", {"p1": 3}),
        (2, "N11", "N12", {"q1": 0}),
        (2, "N12", "N13", {"q1": 0}),
        (3, "N12", "N13", {"p1": 3}),
        (2, "N13", "N14", {"q1": 0}),
        (3, "N13", "N14", {"p1": 3}),
        (2, "N14", "N15", {"q1": 0}),
        (2, "N15", "N1", {}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    constant_properties = [
        ("N1", "N2", {"p1": 1}),
        ("N4", "N5", {"p1": 2}),
        ("N9", "N10", {"p1": 1}),
        ("N10", "N11", {"p1": 1}),
        ("N11", "N12", {"p1": 1}),
        ("N12", "N13", {"p1": 1}),
        ("N13", "N14", {"p1": 1}),
        ("N14", "N15", {"p1": 1}),
        ("N15", "N1", {"p1": 1}),
    ]

    for src, dst, props in constant_properties:
        graph.edge(src, dst).add_constant_properties(props)

    return graph


# For this graph there won't be any temporal property index for property name "p1".
def init_nodes_graph1(graph):
    nodes = [
        (2, "N1", {"q1": 0}),
        (2, "N2", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p1": 1},
        "N2": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    return graph


# For this graph there won't be any constant property index for property name "p1".
def init_nodes_graph2(graph):
    nodes = [
        (1, "N1", {"p1": 1}),
        (2, "N2", {"p1": 1}),
        (3, "N2", {"p1": 2}),
        (2, "N3", {"p1": 2}),
        (3, "N3", {"p1": 1}),
        (2, "N4", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p2": 1},
        "N2": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    return graph


def init_edges_graph(graph):
    edges = [
        (6, "N1", "N2", {"p1": 2}),
        (7, "N1", "N2", {"p1": 1}),
        (6, "N2", "N3", {"p1": 1}),
        (7, "N2", "N3", {"p1": 2}),
        (8, "N3", "N4", {"p1": 1}),
        (9, "N4", "N5", {"p1": 1}),
        (5, "N5", "N6", {"p1": 1}),
        (6, "N5", "N6", {"p1": 2}),
        (5, "N6", "N7", {"p1": 1}),
        (6, "N6", "N7", {"p1": 1}),
        (3, "N7", "N8", {"p1": 1}),
        (5, "N7", "N8", {"p1": 1}),
        (3, "N8", "N9", {"p1": 1}),
        (4, "N8", "N9", {"p1": 2}),
        (2, "N9", "N10", {"p1": 2}),
        (2, "N10", "N11", {"q1": 0}),
        (2, "N10", "N11", {"p1": 3}),
        (2, "N11", "N12", {"p1": 3}),
        (2, "N11", "N12", {"q1": 0}),
        (2, "N12", "N13", {"q1": 0}),
        (3, "N12", "N13", {"p1": 3}),
        (2, "N13", "N14", {"q1": 0}),
        (3, "N13", "N14", {"p1": 3}),
        (2, "N14", "N15", {"q1": 0}),
        (2, "N15", "N1", {}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    constant_properties = [
        ("N1", "N2", {"p1": 1}),
        ("N4", "N5", {"p1": 2}),
        ("N9", "N10", {"p1": 1}),
        ("N10", "N11", {"p1": 1}),
        ("N11", "N12", {"p1": 1}),
        ("N12", "N13", {"p1": 1}),
        ("N13", "N14", {"p1": 1}),
        ("N14", "N15", {"p1": 1}),
        ("N15", "N1", {"p1": 1}),
    ]

    for src, dst, props in constant_properties:
        graph.edge(src, dst).add_constant_properties(props)

    return graph


# For this graph there won't be any constant property index for property name "p1".
def init_edges_graph1(graph):
    edges = [
        (2, "N1", "N2", {"q1": 0}),
        (2, "N2", "N3", {}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    constant_properties = [
        ("N1", "N2", {"p1": 1}),
        ("N2", "N3", {"p1": 1}),
    ]

    for src, dst, props in constant_properties:
        graph.edge(src, dst).add_constant_properties(props)

    return graph


# For this graph there won't be any constant property index for property name "p1".
def init_edges_graph2(graph):
    edges = [
        (2, "N1", "N2", {"p1": 1}),
        (2, "N2", "N3", {"p1": 1}),
        (2, "N2", "N3", {"p1": 2}),
        (2, "N3", "N4", {"p1": 2}),
        (2, "N3", "N4", {"p1": 1}),
        (2, "N4", "N5", {}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    constant_properties = [
        ("N1", "N2", {"p2": 1}),
    ]

    for src, dst, props in constant_properties:
        graph.edge(src, dst).add_constant_properties(props)

    return graph
