import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raphtory import Graph, PersistentGraph

@pytest.mark.skip(reason="Prepares data for debugging purposes")
def test_prepare_data():
    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        "time": [1, 2, 3, 4, 5, 6],
        "type": ["Person 1", "Person 2", "Person 3", "Person 4", "Person 5", "Person 6"],
        "node_type": ["p", "p", "p", "p", "p", "p"],
    }

    table = pa.table(data)
    pq.write_table(table, '/tmp/parquet/nodes.parquet')

    data = {
        "src": [1, 2, 3, 4, 5],
        "dst": [2, 3, 4, 5, 6],
        "time": [1, 2, 3, 4, 5],
        "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
        "marbles": ["red", "blue", "green", "yellow", "purple"],
        "marbles_const": ["red", "blue", "green", "yellow", "purple"],
        "layers": ["layer 1", "layer 2", "layer 3", "layer 4", "layer 5"],
    }

    table = pa.table(data)
    pq.write_table(table, '/tmp/parquet/edges.parquet')


def test_load_from_parquet():
    edges_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'edges.parquet')
    nodes_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'nodes.parquet')

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

    g = Graph()
    g.load_nodes_from_parquet(nodes_parquet_file_path, "id", "time", "node_type", properties=["name"])
    g.load_edges_from_parquet(edges_parquet_file_path, "src", "dst", "time", ["weight", "marbles"], layer="layers")
    assertions(g)

    g.load_node_props_from_parquet(
        nodes_parquet_file_path,
        "id",
        const_properties=["type"],
        shared_const_properties={"tag": "test_tag"},
    )
    assert g.nodes.properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]
    assert g.nodes.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]

    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        const_properties=["marbles_const"],
        shared_const_properties={"tag": "test_tag"},
        layer="layers",
    )
    assert g.layers(
        ["layer 1", "layer 2", "layer 3"]
    ).edges.properties.constant.get("marbles_const").collect() == [
               {"layer 1": "red"},
               {"layer 2": "blue"},
               {"layer 3": "green"},
           ]
    assert g.edges.properties.constant.get("tag").collect() == [
        {"layer 1": "test_tag"},
        {"layer 2": "test_tag"},
        {"layer 3": "test_tag"},
        {"layer 4": "test_tag"},
        {"layer 5": "test_tag"},
    ]
