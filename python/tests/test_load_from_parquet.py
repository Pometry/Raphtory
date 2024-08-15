import os
import re
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raphtory import Graph, PersistentGraph


@pytest.fixture(scope="session")
def parquet_files():
    dirname = tempfile.TemporaryDirectory()
    nodes_parquet_file_path = os.path.join(dirname.name, "parquet", "nodes.parquet")
    edges_parquet_file_path = os.path.join(dirname.name, "parquet", "edges.parquet")
    edge_deletions_parquet_file_path = os.path.join(dirname.name, "parquet", "edges_deletions.parquet")

    os.makedirs(os.path.dirname(nodes_parquet_file_path), exist_ok=True)

    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        "time": [1, 2, 3, 4, 5, 6],
        "type": ["Person 1", "Person 2", "Person 3", "Person 4", "Person 5", "Person 6"],
        "node_type": ["p", "p", "p", "p", "p", "p"],
    }

    table = pa.table(data)
    pq.write_table(table, nodes_parquet_file_path)
    print("""Created nodes.parquet at loc = {}""".format(nodes_parquet_file_path))

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
    pq.write_table(table, edges_parquet_file_path)
    print("""Created edges.parquet at loc = {}""".format(edges_parquet_file_path))

    data = {
        "src": [3, 4],
        "dst": [4, 5],
        "time": [6, 7],
    }

    table = pa.table(data)
    pq.write_table(table, edge_deletions_parquet_file_path)
    print("""Created edges_deletions.parquet at loc = {}""".format(edge_deletions_parquet_file_path))

    yield nodes_parquet_file_path, edges_parquet_file_path, edge_deletions_parquet_file_path

    # Cleanup the temporary directory after tests
    dirname.cleanup()


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
            match=re.escape("Invalid layer: test_layer. Valid layers: _default, layer 1, layer 2, layer 3, layer 4, layer 5"),
    ):
        g.layers(["test_layer"])


def assert_expected_test_layer(g):
    assert g.unique_layers == ["_default", "test_layer"]
    assert g.layers(["test_layer"]).edges.src.id.collect() == [1, 2, 3, 4, 5]


def test_load_from_parquet_graphs(parquet_files):
    nodes_parquet_file_path, edges_parquet_file_path, edges_deletions_parquet_file_path = parquet_files

    g = Graph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_type="node_type",
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = Graph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        time="time",
        node_type="node_type",
        properties=["name"]
    )
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        layer_col="layers"
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)
    assert_expected_layers(g)

    g.load_node_props_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        constant_properties=["type"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assert_expected_node_property_tag(g)
    assert_expected_node_property_type(g)

    g.load_edge_props_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        constant_properties=["marbles_const"],
        shared_constant_properties={"tag": "test_tag"},
        layer_col="layers",
    )
    assert_expected_edge_properties(g)
    assert_expected_layers(g)

    g = Graph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        time="time",
        node_type_col="node_type",
        properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assert_expected_node_types(g)
    assert_expected_node_property_tag(g)

    g = Graph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
        layer_name="test_layer",
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        layer_name="test_layer",
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_constant_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = Graph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        layer_col="layers",
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_constant_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)


def test_load_from_parquet_persistent_graphs(parquet_files):
    nodes_parquet_file_path, edges_parquet_file_path, edges_deletions_parquet_file_path = parquet_files

    g = PersistentGraph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_type="node_type",
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = PersistentGraph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        time="time",
        node_type="node_type",
        properties=["name"]
    )
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        layer_col="layers"
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)
    assert_expected_layers(g)

    g.load_node_props_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        constant_properties=["type"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assert_expected_node_property_tag(g)
    assert_expected_node_property_type(g)

    g.load_edge_props_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        constant_properties=["marbles_const"],
        shared_constant_properties={"tag": "test_tag"},
        layer_col="layers",
    )
    assert_expected_edge_properties(g)
    assert_expected_layers(g)

    g = PersistentGraph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        time="time",
        node_type_col="node_type",
        properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assert_expected_node_types(g)
    assert_expected_node_property_tag(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
        layer_name="test_layer",
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        layer_name="test_layer",
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_constant_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = PersistentGraph.load_from_parquet(
        edge_parquet_path=edges_parquet_file_path,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        layer_col="layers",
        node_parquet_path=nodes_parquet_file_path,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_constant_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
    )
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 3, 4, 5]
    g.load_edges_deletions_from_parquet(
        parquet_path=edges_deletions_parquet_file_path,
        src="src",
        dst="dst",
        time="time"
    )
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 5]

