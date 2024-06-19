import os

import pyarrow as pa
import pyarrow.parquet as pq
from raphtory import Graph, PersistentGraph


def test_load_from_parquet():
    nodes_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'nodes.parquet')
    edges_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'edges.parquet')

    expected_node_ids = [1, 2, 3, 4, 5, 6]
    expected_nodes = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    def assertions(g):
        nodes = []
        for v in g.nodes:
            name = v["name"]
            nodes.append((v.id, name))
        assert g.nodes.id.collect() == expected_node_ids
        assert nodes == expected_nodes

        edges = []
        for e in g.edges:
            weight = e["weight"]
            marbles = e["marbles"]
            edges.append((e.src.id, e.dst.id, weight, marbles))
        assert edges == expected_edges

    g = Graph()
    g.load_nodes_from_parquet(nodes_parquet_file_path, "id", "time", "node_type", properties=["name"])
    g.load_edges_from_parquet(edges_parquet_file_path, "src", "dst", "time", ["weight", "marbles"])
    assertions(g)

    g = Graph.load_from_parquet(
        edge_parquet_file_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_parquet_file_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_type="node_type",
    )
    assertions(g)

#     # g = PersistentGraph.load_from_parquet(
#     #     'test_data.parquet', "src", "dst", "time", ["weight", "marbles"]
#     # )
#     # assertions(g)

def test_load_edges_from_parquet():
    file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'edges.parquet')
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    g = Graph()
    g.load_edges_from_parquet(file_path, "src", "dst", "time", ["weight", "marbles"])

    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))

    assert edges == expected_edges


def test_load_nodes_from_parquet():
    file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'nodes.parquet')
    expected_node_ids = [1, 2, 3, 4, 5, 6]
    expected_nodes = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]

    g = Graph()
    g.load_nodes_from_parquet(file_path, "id", "time", "node_type", properties=["name"])

    nodes = []
    for v in g.nodes:
        name = v["name"]
        nodes.append((v.id, name))

    assert g.nodes.id.collect() == expected_node_ids
    assert nodes == expected_nodes
