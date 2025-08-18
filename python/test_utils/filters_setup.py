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
            {
                "p1": "shivam_kapoor",
                "p9": 5,
                "p10": "Paper_airplane",
                "p20": "Gold_ship",
            },
            "fire_nation",
        ),
        (
            2,
            2,
            {"p1": "prop12", "p2": 2, "p10": "Paper_ship", "p20": "Old_ship"},
            "air_nomads",
        ),
        (3, 1, {"p1": "shivam_kapoor", "p9": 5, "p20": "Gold_ship"}, "fire_nation"),
        (3, 2, {"p20": "Old_ship"}, "air_nomads"),
        (
            3,
            3,
            {"p2": 6, "p3": 1, "p10": "Paper_airplane", "p20": "Gold_boat"},
            "fire_nation",
        ),
        (4, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
        (3, 4, {"p4": "pometry", "p20": "Gold_boat"}, None),
        (4, 4, {"p5": 12, "p20": "Gold_ship"}, None),
    ]

    for time, id, props, node_type in nodes:
        graph.add_node(time, str(id), props, node_type)

    edge_data = [
        (
            1,
            "1",
            "2",
            {"p1": "shivam_kapoor", "p10": "Paper_airplane", "p20": "Gold_ship"},
            "fire_nation",
        ),
        (
            2,
            "1",
            "2",
            {"p1": "shivam_kapoor", "p2": 4, "p20": "Gold_ship"},
            "fire_nation",
        ),
        (
            2,
            "2",
            "3",
            {"p1": "prop12", "p2": 2, "p10": "Paper_ship", "p20": "Gold_boat"},
            "air_nomads",
        ),
        (3, "2", "3", {"p20": "Gold_ship"}, "air_nomads"),
        (3, "3", "1", {"p2": 6, "p3": 1}, "fire_nation"),
        (3, "2", "1", {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, None),
        (
            4,
            "David Gilmour",
            "John Mayer",
            {"p2": 6, "p3": 1, "p20": "Gold_boat"},
            None,
        ),
        (4, "John Mayer", "Jimmy Page", {"p2": 6, "p3": 1, "p20": "Gold_ship"}, None),
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

    metadata = {
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

    for label, props in metadata.items():
        graph.node(label).add_metadata(props)

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

    metadata = [
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

    for src, dst, props in metadata:
        graph.edge(src, dst).add_metadata(props)

    return graph


# For this graph there won't be any temporal property index for property name "p1".
def init_nodes_graph1(graph):
    nodes = [
        (2, "N1", {"q1": 0}),
        (2, "N2", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    metadata = {
        "N1": {"p1": 1},
        "N2": {"p1": 1},
    }

    for label, props in metadata.items():
        graph.node(label).add_metadata(props)

    return graph


# For this graph there won't be any metadata index for property name "p1".
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

    metadata = {
        "N1": {"p2": 1},
        "N2": {"p1": 1},
    }

    for label, props in metadata.items():
        graph.node(label).add_metadata(props)

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

    metadata = [
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

    for src, dst, props in metadata:
        graph.edge(src, dst).add_metadata(props)

    return graph


# For this graph there won't be any metadata index for property name "p1".
def init_edges_graph1(graph):
    edges = [
        (2, "N1", "N2", {"q1": 0}),
        (2, "N2", "N3", {}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    metadata = [
        ("N1", "N2", {"p1": 1}),
        ("N2", "N3", {"p1": 1}),
    ]

    for src, dst, props in metadata:
        graph.edge(src, dst).add_metadata(props)

    return graph


# For this graph there won't be any metadata index for property name "p1".
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

    metadata = [
        ("N1", "N2", {"p2": 1}),
    ]

    for src, dst, props in metadata:
        graph.edge(src, dst).add_metadata(props)

    return graph


import tempfile
from raphtory.graphql import GraphServer
import json
import re

PORT = 1737


def create_test_graph(g):
    g.add_node(
        1,
        "a",
        {
            "prop1": 60,
            "prop2": 31.3,
            "prop3": "abc123",
            "prop4": True,
            "prop5": [1, 2, 3],  # min: 1, max: 3, sum: 6, avg: 2.0, len: 3
            "prop6": [1, 2, 3],  # min: 1, max: 3, sum: 6, avg: 2.0, len: 3
        },
        "fire_nation",
    )
    g.add_node(
        2,
        "a",
        {
            "prop5": [1, 2, 3],  # min: 1, max: 3, sum: 6, avg: 2.0, len: 3
            "prop6": [3, 4, 5],  # min: 3, max: 5, sum: 12, avg: 4.0, len: 3
        },
        "fire_nation",
    )
    g.node("a").add_metadata(
        {
            "prop1": [11, 12, 13],  # min: 11, max: 13, sum: 36, avg: 12.0, len: 3
            "prop2": [1, -2, 3, 0],  # min: -2, max: 3, sum: 2, avg: 0.5, len: 4
        }
    )
    g.add_node(
        1,
        "b",
        {"prop1": 10, "prop2": 31.3, "prop3": "abc223", "prop4": False},
        "fire_nation",
    )
    g.node("b").add_metadata(
        {
            "prop2": [1, -2, 0, 9],  # min: -2, max: 9, sum: 8, avg: 2.0, len: 4
            "prop3": [
                11.0,
                12.0,
                13.0,
            ],  # min: 11.0, max: 13.0, sum: 36.0, avg: 12.0, len: 3
            "prop4": [11, 12],  # min: 11, max: 12, sum: 23, avg: 11.5, len: 2
        }
    )
    g.add_node(
        1,
        "c",
        {
            "prop1": 20,
            "prop2": 31.3,
            "prop3": "abc333",
            "prop4": True,
            "prop5": [5, 6, 7],  # min: 5, max: 7, sum: 18, avg: 6.0, len: 3
            "prop7": [
                "shifu",
                "po",
                "oogway",
            ],  # min: None, max: None, sum: None, avg: None, len: 3
        },
        "water_tribe",
    )
    g.add_node(
        1,
        "d",
        {"prop1": 30, "prop2": 31.3, "prop3": "abc444", "prop4": False},
        "air_nomads",
    )
    g.add_edge(
        2,
        "a",
        "d",
        {
            "eprop1": 60,
            "eprop2": 0.4,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [1, 2, 3],  # min: 1, max: 3, sum: 6, avg: 2.0, len: 3
        },
    )
    g.add_edge(
        2,
        "b",
        "d",
        {
            "eprop1": 10,
            "eprop2": 1.7,
            "eprop3": "xyz123",
            "eprop4": True,
            "eprop5": [3, 4, 5],  # min: 3, max: 5, sum: 12, avg: 4.0, len: 3
        },
    )
    g.add_edge(
        2,
        "c",
        "d",
        {
            "eprop1": 30,
            "eprop2": 6.4,
            "eprop3": "xyz123",
            "eprop4": False,
            "eprop5": [10],  # min: 10, max: 10, sum: 10, avg: 10.0, len: 1
        },
    )
    return g
