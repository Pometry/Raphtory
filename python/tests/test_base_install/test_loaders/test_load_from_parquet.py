import os
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import tempfile
import pandas as pd

from raphtory import Graph, PersistentGraph


@pytest.fixture(scope="session")
def parquet_files():
    dirname = tempfile.TemporaryDirectory()
    nodes_parquet_file_path = os.path.join(dirname.name, "parquet", "nodes.parquet")
    edges_parquet_file_path = os.path.join(dirname.name, "parquet", "edges.parquet")
    edge_deletions_parquet_file_path = os.path.join(
        dirname.name, "parquet", "edges_deletions.parquet"
    )

    os.makedirs(os.path.dirname(nodes_parquet_file_path), exist_ok=True)

    data = {
        "id": [1, 2, 3, 4, 5, 6],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
        "time": [1, 2, 3, 4, 5, 6],
        "type": [
            "Person 1",
            "Person 2",
            "Person 3",
            "Person 4",
            "Person 5",
            "Person 6",
        ],
        "node_type": ["p1", "p2", "p3", "p4", "p5", "p6"],
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
    print(
        """Created edges_deletions.parquet at loc = {}""".format(
            edge_deletions_parquet_file_path
        )
    )

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
    nodes.sort()

    assert g.nodes.id.sorted() == expected_node_ids
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
    edges.sort()
    assert edges == expected_edges


def assert_expected_node_types(g):
    assert g.nodes.node_type == {
        1: "p1",
        2: "p2",
        3: "p3",
        4: "p4",
        5: "p5",
        6: "p6",
    }


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
    assert dict(zip(g.nodes.id, g.nodes.properties.constant.get("type"))) == {
        1: "Person 1",
        2: "Person 2",
        3: "Person 3",
        4: "Person 4",
        5: "Person 5",
        6: "Person 6",
    }


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
    assert dict(
        zip(
            g.layers(["layer 1", "layer 2", "layer 3"]).edges.id,
            g.layers(["layer 1", "layer 2", "layer 3"]).edges.properties.constant.get(
                "marbles_const"
            ),
        )
    ) == {
        (1, 2): {"layer 1": "red"},
        (2, 3): {"layer 2": "blue"},
        (3, 4): {"layer 3": "green"},
    }
    assert dict(zip(g.edges.id, g.edges.properties.constant.get("tag"))) == {
        (1, 2): {"layer 1": "test_tag"},
        (2, 3): {"layer 2": "test_tag"},
        (3, 4): {"layer 3": "test_tag"},
        (4, 5): {"layer 4": "test_tag"},
        (5, 6): {"layer 5": "test_tag"},
    }


def assert_expected_edge_properties_test_layer(g):
    assert g.edges.properties.constant.get("type").collect() == [
        "Edge",
        "Edge",
        "Edge",
        "Edge",
        "Edge",
    ]
    assert g.edges.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]
    assert g.edges.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]


def assert_expected_layers(g):
    assert set(g.unique_layers) == {
        "layer 1",
        "layer 2",
        "layer 3",
        "layer 4",
        "layer 5",
    }
    assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
    assert sorted(g.layers(["layer 1", "layer 2"]).edges.src.id) == [1, 2]
    assert sorted(g.layers(["layer 1", "layer 2", "layer 3"]).edges.src.id) == [
        1,
        2,
        3,
    ]
    assert sorted(g.layers(["layer 1", "layer 4", "layer 5"]).edges.src.id) == [
        1,
        4,
        5,
    ]
    with pytest.raises(
        Exception,
        match=re.escape("Invalid layer: test_layer"),
    ):
        g.layers(["test_layer"])


def assert_expected_test_layer(g):
    assert g.unique_layers == ["test_layer"]
    assert sorted(g.layers(["test_layer"]).edges.src.id) == [1, 2, 3, 4, 5]


def test_load_from_parquet_graphs(parquet_files):
    (
        nodes_parquet_file_path,
        edges_parquet_file_path,
        edges_deletions_parquet_file_path,
    ) = parquet_files

    g = Graph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        node_type_col="node_type",
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = Graph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        node_type_col="node_type",
        properties=["name"],
    )
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        layer_col="layers",
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
        layer="test_layer",
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        time="time",
        src="src",
        dst="dst",
        layer="test_layer",
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        shared_constant_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = Graph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        layer_col="layers",
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        constant_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)


def test_load_from_parquet_persistent_graphs(parquet_files):
    (
        nodes_parquet_file_path,
        edges_parquet_file_path,
        edges_deletions_parquet_file_path,
    ) = parquet_files

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        node_type_col="node_type",
    )
    assert_expected_nodes(g)
    assert_expected_edges(g)

    g = PersistentGraph()
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        id="id",
        time="time",
        node_type="node_type",
        properties=["name"],
    )
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        properties=["weight", "marbles"],
        layer_col="layers",
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
        time="time",
        id="id",
        node_type_col="node_type",
        properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assert_expected_node_types(g)
    assert_expected_node_property_tag(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
    )
    assert_expected_edge_properties_test_layer(g)
    assert_expected_test_layer(g)

    g = Graph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        layer="test_layer",
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        shared_constant_properties={"dept": "Sales"},
    )
    assert_expected_test_layer(g)
    assert_expected_node_property_dept(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        src="src",
        dst="dst",
        time="time",
        layer_col="layers",
    )
    g.load_nodes_from_parquet(
        parquet_path=nodes_parquet_file_path,
        time="time",
        id="id",
        properties=["name"],
        constant_properties=["type"],
    )
    assert_expected_node_property_type(g)
    assert_expected_layers(g)

    g = PersistentGraph()
    g.load_edges_from_parquet(
        parquet_path=edges_parquet_file_path,
        time="time",
        src="src",
        dst="dst",
    )
    assert set(g.window(10, 12).edges.id) == {(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)}
    g.load_edge_deletions_from_parquet(
        parquet_path=edges_deletions_parquet_file_path,
        time="time",
        src="src",
        dst="dst",
    )
    assert set(g.window(10, 12).edges.id) == {(1, 2), (2, 3), (5, 6)}


def test_edge_both_option_failures_parquet(parquet_files):
    (
        nodes_parquet_file_path,
        edges_parquet_file_path,
        edges_deletions_parquet_file_path,
    ) = parquet_files
    # CHECK ALL EDGE FUNCTIONS ON GRAPH FAIL WITH BOTH LAYER AND LAYER_COL
    g = Graph()
    with pytest.raises(
        Exception,
        match=r"Failed to load graph: Failed to load graph WrongNumOfArgs\(\"layer_name\", \"layer_col\"\)",
    ):
        g.load_edges_from_parquet(
            edges_parquet_file_path,
            "time",
            "src",
            "dst",
            layer="blah",
            layer_col="marbles",
        )

    with pytest.raises(
        Exception,
        match=r"Failed to load graph: Failed to load graph WrongNumOfArgs\(\"layer_name\", \"layer_col\"\)",
    ):
        g.load_edge_props_from_parquet(
            edges_parquet_file_path, "src", "dst", layer="blah", layer_col="marbles"
        )

    # CHECK IF JUST LAYER WORKS
    g = Graph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer="blah"
    )
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]

    g = Graph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer="blah"
    )
    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        layer="blah",
        constant_properties=["marbles"],
    )
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]
    assert dict(
        zip(g.layer("blah").edges.id, g.layer("blah").edges.properties.get("marbles"))
    ) == {
        (1, 2): "red",
        (2, 3): "blue",
        (3, 4): "green",
        (4, 5): "yellow",
        (5, 6): "purple",
    }

    # CHECK IF JUST LAYER_COL WORKS
    g = Graph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer_col="marbles"
    )
    assert dict(zip(g.edges.id, g.edges.layer_names)) == {
        (1, 2): ["red"],
        (2, 3): ["blue"],
        (3, 4): ["green"],
        (4, 5): ["yellow"],
        (5, 6): ["purple"],
    }
    assert set(g.unique_layers) == {
        "red",
        "blue",
        "green",
        "yellow",
        "purple",
    }

    g = Graph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer_col="marbles"
    )
    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        layer_col="marbles",
        constant_properties=["marbles"],
    )
    assert dict(zip(g.edges.id, g.edges.layer_names)) == {
        (1, 2): ["red"],
        (2, 3): ["blue"],
        (3, 4): ["green"],
        (4, 5): ["yellow"],
        (5, 6): ["purple"],
    }
    assert set(g.unique_layers) == {
        "red",
        "blue",
        "green",
        "yellow",
        "purple",
    }
    assert dict(zip(g.edges.id, g.edges.properties.get("marbles"))) == {
        (1, 2): {"red": "red"},
        (2, 3): {"blue": "blue"},
        (3, 4): {"green": "green"},
        (4, 5): {"yellow": "yellow"},
        (5, 6): {"purple": "purple"},
    }

    g = PersistentGraph()
    with pytest.raises(
        Exception,
        match=r"Failed to load graph: Failed to load graph WrongNumOfArgs\(\"layer_name\", \"layer_col\"\)",
    ):
        g.load_edges_from_parquet(
            edges_parquet_file_path,
            "time",
            "src",
            "dst",
            layer="blah",
            layer_col="marbles",
        )

    with pytest.raises(
        Exception,
        match=r"Failed to load graph: Failed to load graph WrongNumOfArgs\(\"layer_name\", \"layer_col\"\)",
    ):
        g.load_edge_props_from_parquet(
            edges_parquet_file_path, "src", "dst", layer="blah", layer_col="marbles"
        )

    with pytest.raises(
        Exception,
        match=r"Failed to load graph: Failed to load graph WrongNumOfArgs\(\"layer_name\", \"layer_col\"\)",
    ):
        g.load_edge_deletions_from_parquet(
            edges_parquet_file_path,
            "time",
            "src",
            "dst",
            layer="blah",
            layer_col="marbles",
        )

    # CHECK IF JUST LAYER WORKS
    g = PersistentGraph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer="blah"
    )
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]

    g = PersistentGraph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer="blah"
    )
    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        layer="blah",
        constant_properties=["marbles"],
    )
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]
    assert dict(
        zip(g.layer("blah").edges.id, g.layer("blah").edges.properties.get("marbles"))
    ) == {
        (1, 2): "red",
        (2, 3): "blue",
        (3, 4): "green",
        (4, 5): "yellow",
        (5, 6): "purple",
    }

    g = PersistentGraph()
    g.load_edge_deletions_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer="blah"
    )
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]

    # CHECK IF JUST LAYER_COL WORKS
    g = PersistentGraph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer_col="marbles"
    )
    assert dict(zip(g.edges.id, g.edges.layer_names)) == {
        (1, 2): ["red"],
        (2, 3): ["blue"],
        (3, 4): ["green"],
        (4, 5): ["yellow"],
        (5, 6): ["purple"],
    }
    assert set(g.unique_layers) == {
        "red",
        "blue",
        "green",
        "yellow",
        "purple",
    }

    g = PersistentGraph()
    g.load_edges_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer_col="marbles"
    )
    g.load_edge_props_from_parquet(
        edges_parquet_file_path,
        "src",
        "dst",
        layer_col="marbles",
        constant_properties=["marbles"],
    )
    assert dict(zip(g.edges.id, g.edges.layer_names)) == {
        (1, 2): ["red"],
        (2, 3): ["blue"],
        (3, 4): ["green"],
        (4, 5): ["yellow"],
        (5, 6): ["purple"],
    }
    assert set(g.unique_layers) == {
        "red",
        "blue",
        "green",
        "yellow",
        "purple",
    }
    assert dict(zip(g.edges.id, g.edges.properties.get("marbles"))) == {
        (1, 2): {"red": "red"},
        (2, 3): {"blue": "blue"},
        (3, 4): {"green": "green"},
        (4, 5): {"yellow": "yellow"},
        (5, 6): {"purple": "purple"},
    }

    g = PersistentGraph()
    g.load_edge_deletions_from_parquet(
        edges_parquet_file_path, "time", "src", "dst", layer_col="marbles"
    )
    assert dict(zip(g.edges.id, g.edges.layer_names)) == {
        (1, 2): ["red"],
        (2, 3): ["blue"],
        (3, 4): ["green"],
        (4, 5): ["yellow"],
        (5, 6): ["purple"],
    }
    assert set(g.unique_layers) == {
        "red",
        "blue",
        "green",
        "yellow",
        "purple",
    }


def test_node_both_option_failures_parquet(parquet_files):
    (
        nodes_parquet_file_path,
        edges_parquet_file_path,
        edges_deletions_parquet_file_path,
    ) = parquet_files

    # CHECK ALL NODE FUNCTIONS ON GRAPH FAIL WITH BOTH NODE_TYPE AND NODE_TYPE_COL
    with pytest.raises(
        Exception,
        match=re.escape(
            r'Failed to load graph: Failed to load graph WrongNumOfArgs("node_type_name", "node_type_col")'
        ),
    ):
        g = Graph()
        g.load_nodes_from_parquet(
            nodes_parquet_file_path,
            "time",
            "id",
            node_type="node_type",
            node_type_col="node_type",
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            r'Failed to load graph: Failed to load graph WrongNumOfArgs("node_type_name", "node_type_col")'
        ),
    ):
        g = Graph()
        g.load_node_props_from_parquet(
            nodes_parquet_file_path,
            "id",
            node_type="node_type",
            node_type_col="node_type",
        )

    # CHECK IF JUST NODE_TYPE WORKS
    g = Graph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path, "time", "id", node_type="node_type"
    )
    assert g.nodes.node_type.collect() == [
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
    ]
    g = Graph()
    g.load_nodes_from_parquet(nodes_parquet_file_path, "time", "id")
    g.load_node_props_from_parquet(nodes_parquet_file_path, "id", node_type="node_type")
    assert g.nodes.node_type.collect() == [
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
    ]

    # CHECK IF JUST NODE_TYPE_COL WORKS
    g = Graph()
    g.load_nodes_from_parquet(
        nodes_parquet_file_path, "time", "id", node_type_col="node_type"
    )
    assert g.nodes.node_type.sorted_by_id() == ["p1", "p2", "p3", "p4", "p5", "p6"]
    g = Graph()
    g.load_nodes_from_parquet(nodes_parquet_file_path, "time", "id")
    g.load_node_props_from_parquet(
        nodes_parquet_file_path, "id", node_type_col="node_type"
    )
    assert g.nodes.node_type.sorted_by_id() == ["p1", "p2", "p3", "p4", "p5", "p6"]
