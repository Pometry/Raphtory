import os
import re

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


edges_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'edges.parquet')
nodes_parquet_file_path = os.path.join(os.path.dirname(__file__), 'data', 'parquet', 'nodes.parquet')


def assert_expected_nodes(g):
    expected_node_ids = [1, 2, 3, 4, 5, 6]
    expected_nodes = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]
    nodes = []
    for v in g.nodes:
        name = v["name"]
        nodes.append((v.id, name))
    assert g.nodes.id.collect() == expected_node_ids
    assert nodes == expected_nodes


def assert_expected_edges(g):
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]
    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))
    assert edges == expected_edges


def assert_expected_node_types(g):
    assert g.nodes.node_type == [
        "p",
        "p",
        "p",
        "p",
        "p",
        "p",
    ]


def assert_expected_node_property_tag(g):
    assert g.nodes.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]


def assert_expected_node_property_type(g):
    assert g.nodes.properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]


def assert_expected_node_property_dept(g):
    assert g.nodes.properties.constant.get("dept").collect() == [
        "Sales",
        "Sales",
        "Sales",
        "Sales",
        "Sales",
        "Sales",
    ]


def assert_expected_edge_properties(g):
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


def assert_expected_edge_properties_test_layer(g):
    assert g.edges.properties.constant.get("type").collect() == [
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
    ]
    assert g.edges.properties.constant.get("tag").collect() == [
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
    ]
    assert g.edges.properties.constant.get("tag").collect() == [
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
    ]


def assert_expected_layers(g):
    assert g.unique_layers == ["_default", "layer 1", "layer 2", "layer 3", "layer 4", "layer 5"]
    assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
    assert g.layers(["layer 1", "layer 2"]).edges.src.id.collect() == [1, 2]
    assert g.layers(["layer 1", "layer 2", "layer 3"]).edges.src.id.collect() == [1, 2, 3]
    assert g.layers(["layer 1", "layer 4", "layer 5"]).edges.src.id.collect() == [1, 4, 5]
    with pytest.raises(
            Exception,
            match=re.escape("Invalid layer test_layer."),
    ):
        g.layers(["test_layer"])


def assert_expected_test_layer(g):
    assert g.unique_layers == ["_default", "test_layer"]
    assert g.layers(["test_layer"]).edges.src.id.collect() == [1, 2, 3, 4, 5]


def test_load_from_parquet_graphs():
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
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = Graph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path,
        "id",
        "time",
        "node_type",
        properties=["name"]
    )
    g.load_edges_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        "time",
        ["weight", "marbles"],
        layer="layers"
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)
    assert_expected_layers(g)

    g.load_node_props_from_parquet(
        nodes_parquet_file_path,
        "id",
        const_properties=["type"],
        shared_const_properties={"tag": "test_tag"},
    )
    assert_expected_node_property_tag(g)
    assert_expected_node_property_type(g)

    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        const_properties=["marbles_const"],
        shared_const_properties={"tag": "test_tag"},
        layer="layers",
    )
    assert_expected_edge_properties(g)
    assert_expected_layers(g)

    g = Graph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path,
        "id",
        "time",
        "node_type",
        properties=["name"],
        shared_const_properties={"tag": "test_tag"},
    )
    assert_expected_node_types(g)
    assert_expected_node_property_tag(g)

    g = Graph()
    g.load_edges_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        "time",
        properties=["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
        layer_in_df=False,
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph.load_from_parquet(
        edge_parquet_file_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_layer="test_layer",
        layer_in_df=False,
        node_parquet_file_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_const_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = Graph.load_from_parquet(
        edge_parquet_file_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_layer="layers",
        node_parquet_file_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_const_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)


def test_load_from_parquet_persistent_graphs():
    g = PersistentGraph.load_from_parquet(
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
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = PersistentGraph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path,
        "id",
        "time",
        "node_type",
        properties=["name"]
    )
    g.load_edges_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        "time",
        ["weight", "marbles"],
        layer="layers"
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)
    assert_expected_layers(g)

    g.load_node_props_from_parquet(
        nodes_parquet_file_path,
        "id",
        const_properties=["type"],
        shared_const_properties={"tag": "test_tag"},
    )
    assert_expected_node_property_tag(g)
    assert_expected_node_property_type(g)

    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        const_properties=["marbles_const"],
        shared_const_properties={"tag": "test_tag"},
        layer="layers",
    )
    assert_expected_edge_properties(g)
    assert_expected_layers(g)

    g = PersistentGraph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path,
        "id",
        "time",
        "node_type",
        properties=["name"],
        shared_const_properties={"tag": "test_tag"},
    )
    assert_expected_node_types(g)
    assert_expected_node_property_tag(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        "time",
        properties=["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
        layer_in_df=False,
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph.load_from_parquet(
        edge_parquet_file_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_layer="test_layer",
        layer_in_df=False,
        node_parquet_file_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_const_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = PersistentGraph.load_from_parquet(
        edge_parquet_file_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_layer="layers",
        node_parquet_file_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_const_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)
