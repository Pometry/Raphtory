import math
import re
import sys

import numpy
import pandas as pd
import pandas.core.frame
import pyarrow as pa
import pytest
from raphtory import Graph, PersistentGraph
from raphtory import algorithms
from raphtory import graph_loader
import tempfile
from math import isclose
import datetime


def test_load_from_pandas():
    import pandas as pd

    df = pd.DataFrame(
        {
            "time": [1, 2, 3, 4, 5],
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )

    expected_nodes = [1, 2, 3, 4, 5, 6]
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    def assertions(g):
        edges = []
        for e in g.edges:
            weight = e["weight"]
            marbles = e["marbles"]
            edges.append((e.src.id, e.dst.id, weight, marbles))

        edges.sort()

        assert g.nodes.id.sorted() == expected_nodes
        assert edges == expected_edges

    g = Graph()
    g.load_edges_from_pandas(df, "time", "src", "dst", ["weight", "marbles"])
    assertions(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(df, "time", "src", "dst", ["weight", "marbles"])
    assertions(g)


def test_load_from_pandas_with_invalid_data():
    # Create a DataFrame with invalid data
    df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, "3.0 KG", 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )

    def assertions(exc_info):
        assert "ArrowInvalid" in str(exc_info.value)
        assert (
            "Could not convert '3.0 KG' with type str: tried to convert to double"
            in str(exc_info.value)
        )

    # Use pytest.raises to expect an exception
    with pytest.raises(Exception) as exc_info:
        g = Graph()
        g.load_edges_from_pandas(df, "time", "src", "dst", ["weight", "marbles"])
    assertions(exc_info)

    with pytest.raises(Exception) as exc_info:
        g = PersistentGraph()
        g.load_edges_from_pandas(df, "time", "src", "dst", ["weight", "marbles"])
    assertions(exc_info)

    # Optionally, you can check the exception message or type
    assert "ArrowInvalid" in str(exc_info.value)
    assert (
        "Could not convert '3.0 KG' with type str: tried to convert to double"
        in str(exc_info.value)
    )


def test_load_from_pandas_into_existing_graph():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )

    nodes_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
            "node_type": ["p", "p", "p", "p", "p", "p"],
        }
    )

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
        edges = []
        for e in g.edges:
            weight = e["weight"]
            marbles = e["marbles"]
            edges.append((e.src.id, e.dst.id, weight, marbles))
        edges.sort()

        nodes = []
        for v in g.nodes:
            name = v["name"]
            nodes.append((v.id, name))
        nodes.sort()

        assert g.nodes.id.sorted() == expected_node_ids
        assert edges == expected_edges
        assert nodes == expected_nodes

    g = Graph()
    g.load_nodes_from_pandas(
        nodes_df, "time", "id", node_type_col="node_type", properties=["name"]
    )
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", ["weight", "marbles"])
    assertions(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(
        nodes_df, "time", "id", node_type_col="node_type", properties=["name"]
    )
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", ["weight", "marbles"])
    assertions(g)


def test_load_from_pandas_nodes():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )
    nodes_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
            "node_type": ["P", "P", "P", "P", "P", "P"],
        }
    )

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
        edges = []
        for e in g.edges:
            weight = e["weight"]
            marbles = e["marbles"]
            edges.append((e.src.id, e.dst.id, weight, marbles))
        edges.sort()

        nodes = []
        for v in g.nodes:
            name = v["name"]
            nodes.append((v.id, name))
        nodes.sort()

        assert nodes == expected_nodes
        assert g.nodes.id.sorted() == expected_node_ids
        assert edges == expected_edges

    g = Graph()
    g.load_edges_from_pandas(
        edges_df, time="time", src="src", dst="dst", properties=["weight", "marbles"]
    )
    g.load_nodes_from_pandas(
        df=nodes_df, time="time", id="id", properties=["name"], node_type="node_type"
    )
    assertions(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(
        edges_df, time="time", src="src", dst="dst", properties=["weight", "marbles"]
    )
    g.load_nodes_from_pandas(
        df=nodes_df, time="time", id="id", properties=["name"], node_type="node_type"
    )

    assertions(g)


def test_load_from_pandas_with_types():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
            "marbles_const": ["red", "blue", "green", "yellow", "purple"],
            "layers": ["layer 1", "layer 2", "layer 3", "layer 4", "layer 5"],
        }
    )
    nodes_df = pd.DataFrame(
        {
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
            "node_type": [
                "Person",
                "Person",
                "Person",
                "Person",
                "Person",
                "Person",
            ],
        }
    )

    def assertions1(g):
        assert g.nodes.node_type == [
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
        ]
        assert g.nodes.metadata.get("tag").collect() == [
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
        ]

    g = Graph()
    g.load_nodes_from_pandas(
        nodes_df,
        "time",
        "id",
        node_type_col="node_type",
        properties=["name"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions1(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(
        nodes_df,
        "time",
        "id",
        node_type_col="node_type",
        properties=["name"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions1(g)

    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        node_type_col="node_type",
        metadata=["name"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions1(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        node_type_col="node_type",
        metadata=["name"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions1(g)

    def assertions2(g):
        assert dict(zip(g.nodes.id, g.nodes.metadata.get("type"))) == {
            1: "Person 1",
            2: "Person 2",
            3: "Person 3",
            4: "Person 4",
            5: "Person 5",
            6: "Person 6",
        }

    g = Graph()
    g.load_nodes_from_pandas(
        nodes_df,
        "time",
        "id",
        node_type_col="node_type",
        properties=["name"],
        metadata=["type"],
    )
    assertions2(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(
        nodes_df,
        "id",
        "time",
        node_type_col="node_type",
        properties=["name"],
        metadata=["type"],
    )
    assertions2(g)

    def assertions3(g):
        assert g.unique_layers == ["test_layer"]
        assert set(g.layers(["test_layer"]).edges.src.id) == {1, 2, 3, 4, 5}
        assert g.edges.metadata.get("type").collect() == [
            "Edge",
            "Edge",
            "Edge",
            "Edge",
            "Edge",
        ]
        assert g.edges.metadata.get("tag").collect() == [
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
        ]
        assert dict(zip(g.edges.id, g.edges.metadata.get("marbles_const"))) == {
            (1, 2): "red",
            (2, 3): "blue",
            (3, 4): "green",
            (4, 5): "yellow",
            (5, 6): "purple",
        }

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        properties=["weight", "marbles"],
        metadata=["marbles_const"],
        shared_metadata={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
    )
    assertions3(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        properties=["weight", "marbles"],
        metadata=["marbles_const"],
        shared_metadata={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
    )
    assertions3(g)

    def assertions4(g):
        assert g.layers(["layer 1"]).edges.id.collect() == [(1, 2)]
        assert set(g.layers(["layer 1", "layer 2"]).edges.id) == {(1, 2), (2, 3)}
        assert set(g.layers(["layer 1", "layer 2", "layer 3"]).edges.id) == {
            (1, 2),
            (2, 3),
            (3, 4),
        }
        assert set(g.layers(["layer 1", "layer 4", "layer 5"]).edges.id) == {
            (1, 2),
            (4, 5),
            (5, 6),
        }

    g = Graph()
    g.load_edges_from_pandas(
        edges_df, "time", "src", "dst", ["weight", "marbles"], layer_col="layers"
    )
    assertions4(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(
        edges_df, "time", "src", "dst", ["weight", "marbles"], layer_col="layers"
    )
    assertions4(g)

    def assertions5(g):
        assert g.nodes.metadata.get("type").collect() == [
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
        ]
        assert set(g.layers(["test_layer"]).edges.id) == {
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
        }

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        layer="test_layer",
    )
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        shared_metadata={"type": "Person"},
    )
    assertions5(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        layer="test_layer",
    )
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        shared_metadata={"type": "Person"},
    )
    assertions5(g)

    def assertions6(g):
        assert dict(zip(g.nodes.id, g.nodes.metadata.get("type"))) == {
            1: "Person 1",
            2: "Person 2",
            3: "Person 3",
            4: "Person 4",
            5: "Person 5",
            6: "Person 6",
        }
        assert g.layers(["layer 1"]).edges.id.collect() == [(1, 2)]
        assert set(g.layers(["layer 1", "layer 2"]).edges.id) == {(1, 2), (2, 3)}
        assert set(g.layers(["layer 1", "layer 2", "layer 3"]).edges.id) == {
            (1, 2),
            (2, 3),
            (3, 4),
        }
        assert set(g.layers(["layer 1", "layer 4", "layer 5"]).edges.id) == {
            (1, 2),
            (4, 5),
            (5, 6),
        }

    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="layers")
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        metadata=["type"],
    )
    assertions6(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="layers")
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        metadata=["type"],
    )
    assertions6(g)

    def assertions7(g):
        assert dict(zip(g.nodes.id, g.nodes.metadata.get("type"))) == {
            1: "Person 1",
            2: "Person 2",
            3: "Person 3",
            4: "Person 4",
            5: "Person 5",
            6: "Person 6",
        }
        assert g.nodes.metadata.get("tag").collect() == [
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
            "test_tag",
        ]

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
        layer_col="layers",
    )
    g.load_nodes_from_pandas(df=nodes_df, time="time", id="id", properties=["name"])
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        metadata=["type"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions7(g)

    def assertions8(g):
        assert dict(
            zip(
                g.layers(["layer 1", "layer 2", "layer 3"]).edges.id,
                g.layers(["layer 1", "layer 2", "layer 3"]).edges.metadata.get(
                    "marbles_const"
                ),
            )
        ) == {
            (1, 2): {"layer 1": "red"},
            (2, 3): {"layer 2": "blue"},
            (3, 4): {"layer 3": "green"},
        }
        assert dict(zip(g.edges.id, g.edges.metadata.get("tag"))) == {
            (1, 2): {"layer 1": "test_tag"},
            (2, 3): {"layer 2": "test_tag"},
            (3, 4): {"layer 3": "test_tag"},
            (4, 5): {"layer 4": "test_tag"},
            (5, 6): {"layer 5": "test_tag"},
        }

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        metadata=["marbles_const"],
        shared_metadata={"tag": "test_tag"},
        layer_col="layers",
    )
    assertions8(g)

    g = PersistentGraph()

    g.load_edges_from_pandas(
        edges_df,
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
        layer_col="layers",
    )
    g.load_nodes_from_pandas(df=nodes_df, time="time", id="id", properties=["name"])
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        metadata=["type"],
        shared_metadata={"tag": "test_tag"},
    )
    assertions7(g)

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        metadata=["marbles_const"],
        shared_metadata={"tag": "test_tag"},
        layer_col="layers",
    )
    assertions8(g)

    def assertions_layers_in_df(g):
        assert set(g.unique_layers) == {
            "layer 1",
            "layer 2",
            "layer 3",
            "layer 4",
            "layer 5",
        }
        assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
        assert g.layers(["layer 3"]).edges.src.id.collect() == [3]
        with pytest.raises(
            Exception,
            match=re.escape("Invalid layer: test_layer"),
        ):
            g.layers(["test_layer"])

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        ["weight", "marbles"],
        metadata=["marbles_const"],
        shared_metadata={"type": "Edge", "tag": "test_tag"},
        layer_col="layers",
    )
    assertions_layers_in_df(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        ["weight", "marbles"],
        metadata=["marbles_const"],
        shared_metadata={"type": "Edge", "tag": "test_tag"},
        layer_col="layers",
    )
    assertions_layers_in_df(g)


def test_missing_columns():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )
    nodes_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
            "node_type": ["P", "P", "P", "P", "P", "P"],
        }
    )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_src, not_dst, not_time"
        ),
    ):
        g = Graph()
        g.load_edges_from_pandas(
            edges_df,
            time="not_time",
            src="not_src",
            dst="not_dst",
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_src, not_dst, not_time"
        ),
    ):
        g = PersistentGraph()
        g.load_edges_from_pandas(
            edges_df,
            time="not_time",
            src="not_src",
            dst="not_dst",
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_weight, bleep_bloop"
        ),
    ):
        g = Graph()
        g.load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["not_weight", "marbles"],
            metadata=["bleep_bloop"],
        )
        g.load_nodes_from_pandas(df=nodes_df, time="time", id="id", properties=["name"])

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_weight, bleep_bloop"
        ),
    ):
        g = PersistentGraph()
        g.load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["not_weight", "marbles"],
            metadata=["bleep_bloop"],
        )
        g.load_nodes_from_pandas(df=nodes_df, time="time", id="id", properties=["name"])

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_id, not_time, not_name"
        ),
    ):
        g = Graph()
        g.load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["weight", "marbles"],
        )
        g.load_nodes_from_pandas(
            df=nodes_df, time="not_time", id="not_id", properties=["not_name"]
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_id, not_time, not_name"
        ),
    ):
        g = PersistentGraph()
        g.load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["weight", "marbles"],
        )
        g.load_nodes_from_pandas(
            df=nodes_df, id="not_id", time="not_time", properties=["not_name"]
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, dist, wait, marples"
        ),
    ):
        g = Graph()
        g.load_edge_props_from_pandas(
            edges_df,
            src="sauce",
            dst="dist",
            metadata=["wait", "marples"],
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, dist, wait, marples"
        ),
    ):
        g = PersistentGraph()
        g.load_edge_props_from_pandas(
            edges_df,
            src="sauce",
            dst="dist",
            metadata=["wait", "marples"],
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, wait, marples"
        ),
    ):
        g = Graph()
        g.load_node_props_from_pandas(
            nodes_df,
            id="sauce",
            metadata=["wait", "marples"],
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, wait, marples"
        ),
    ):
        g = PersistentGraph()
        g.load_node_props_from_pandas(
            nodes_df,
            id="sauce",
            metadata=["wait", "marples"],
        )


def test_none_columns_edges():
    edges_df = pd.DataFrame(
        {"src": [1, None, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Float64 not supported as node id type")
    ):
        g = Graph()
        g.load_edges_from_pandas(edges_df, "time", "src", "dst")

    with pytest.raises(
        Exception, match=re.escape("Float64 not supported as node id type")
    ):
        PersistentGraph().load_edges_from_pandas(edges_df, "time", "src", "dst")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, None, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Float64 not supported as node id type")
    ):
        Graph().load_edges_from_pandas(edges_df, "time", "src", "dst")
    with pytest.raises(
        Exception, match=re.escape("Float64 not supported as node id type")
    ):
        PersistentGraph().load_edges_from_pandas(edges_df, "time", "src", "dst")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, None, 4, 5]}
    )
    with pytest.raises(Exception, match=re.escape("Missing value for timestamp")):
        Graph().load_edges_from_pandas(edges_df, "time", "src", "dst")
    with pytest.raises(Exception, match=re.escape("Missing value for timestamp")):
        PersistentGraph().load_edges_from_pandas(edges_df, "time", "src", "dst")


def test_loading_list_as_properties():
    df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, "2.0", 3.0, 4.0, 5.0],
            "marbles": [["red"], ["blue"], ["green"], ["yellow"], ["purple"]],
        }
    )

    g = Graph()
    g.load_edges_from_pandas(
        df,
        time="time",
        src="src",
        dst="dst",
        properties=["marbles"],
    )

    assert g.edge(1, 2).properties["marbles"] == ["red"]

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "time": [1, 2, 3, 4, 5],
            "marbles": [["red"], ["blue"], ["green"], ["yellow"], ["purple"]],
        }
    )

    g = Graph()
    g.load_nodes_from_pandas(
        df=df,
        time="time",
        id="id",
        properties=["marbles"],
    )

    assert g.node(2).properties["marbles"] == ["blue"]


def test_unparsable_props():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, "2.0", 3.0, 4.0, 5.0],
            "marbles": [["red"], ["blue"], ["green"], ["yellow"], ["purple"]],
        }
    )

    with pytest.raises(
        Exception,
        match=re.escape(
            """"Could not convert '2.0' with type str: tried to convert to double", 'Conversion failed for column weight with type object'"""
        ),
    ):
        Graph().load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["weight"],
        )
    with pytest.raises(
        Exception,
        match=re.escape(
            """"Could not convert '2.0' with type str: tried to convert to double", 'Conversion failed for column weight with type object'"""
        ),
    ):
        PersistentGraph().load_edges_from_pandas(
            edges_df,
            time="time",
            src="src",
            dst="dst",
            properties=["weight"],
        )


def test_load_node_from_pandas_with_node_types():
    nodes_df = pd.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "time": [1, 2, 3, 4],
        }
    )
    nodes_df2 = pd.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "time": [1, 2, 3, 4],
            "node_type": ["node_type1", "node_type2", "node_type3", "node_type4"],
        }
    )
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
        }
    )

    def nodes_assertions(g):
        assert g.get_all_node_types() == []
        assert g.count_nodes() == 4

    def nodes_assertions2(g):
        assert g.get_all_node_types() == ["node_type"]
        assert g.count_nodes() == 4

    def nodes_assertions3(g):
        all_node_types = g.get_all_node_types()
        all_node_types.sort()
        assert all_node_types == [
            "node_type1",
            "node_type2",
            "node_type3",
            "node_type4",
        ]
        assert g.count_nodes() == 4

    def edges_assertions(g):
        assert g.get_all_node_types() == []
        assert g.count_nodes() == 6

    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    nodes_assertions(g)
    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    nodes_assertions(g)

    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id", node_type="node_type")
    nodes_assertions2(g)
    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df, "time", "id", node_type="node_type")
    nodes_assertions2(g)

    g = Graph()
    g.load_nodes_from_pandas(nodes_df2, "time", "id", node_type="node_type")
    nodes_assertions2(g)
    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df2, "time", "id", node_type="node_type")
    nodes_assertions2(g)

    g = Graph()
    g.load_nodes_from_pandas(nodes_df2, "time", "id", node_type_col="node_type")
    nodes_assertions3(g)
    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df2, "time", "id", node_type_col="node_type")
    nodes_assertions3(g)

    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst")
    edges_assertions(g)
    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst")
    edges_assertions(g)


def test_load_edge_deletions_from_pandas():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
        }
    )
    edge_dels_df = pd.DataFrame(
        {
            "src": [3, 4],
            "dst": [4, 5],
            "time": [6, 7],
        }
    )

    g = PersistentGraph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst")
    assert set(g.window(10, 12).edges.id) == {(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)}
    g.load_edge_deletions_from_pandas(edge_dels_df, "time", "src", "dst")
    assert set(g.window(10, 12).edges.src.id) == {1, 2, 5}


def test_edge_both_option_failures_pandas():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )
    # CHECK ALL EDGE FUNCTIONS ON GRAPH FAIL WITH BOTH LAYER AND LAYER_COL
    g = Graph()
    with pytest.raises(
        Exception,
        match=r"You cannot set ‘layer_name’ and ‘layer_col’ at the same time. Please pick one or the other.",
    ):
        g.load_edges_from_pandas(
            edges_df, "time", "src", "dst", layer="blah", layer_col="marbles"
        )

    with pytest.raises(
        Exception,
        match=r"You cannot set ‘layer_name’ and ‘layer_col’ at the same time. Please pick one or the other.",
    ):
        g.load_edge_props_from_pandas(
            edges_df, "src", "dst", layer="blah", layer_col="marbles"
        )

    # CHECK IF JUST LAYER WORKS
    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer="blah")
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]

    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer="blah")
    g.load_edge_props_from_pandas(
        edges_df, "src", "dst", layer="blah", metadata=["marbles"]
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
        zip(g.layer("blah").edges.id, g.layer("blah").edges.metadata.get("marbles"))
    ) == {
        (1, 2): "red",
        (2, 3): "blue",
        (3, 4): "green",
        (4, 5): "yellow",
        (5, 6): "purple",
    }

    # CHECK IF JUST LAYER_COL WORKS
    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="marbles")
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
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="marbles")
    g.load_edge_props_from_pandas(
        edges_df, "src", "dst", layer_col="marbles", metadata=["marbles"]
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
    assert dict(zip(g.edges.id, g.edges.metadata.get("marbles"))) == {
        (1, 2): {"red": "red"},
        (2, 3): {"blue": "blue"},
        (3, 4): {"green": "green"},
        (4, 5): {"yellow": "yellow"},
        (5, 6): {"purple": "purple"},
    }

    g = PersistentGraph()
    with pytest.raises(
        Exception,
        match=r"You cannot set ‘layer_name’ and ‘layer_col’ at the same time. Please pick one or the other.",
    ):
        g.load_edges_from_pandas(
            edges_df, "time", "src", "dst", layer="blah", layer_col="marbles"
        )

    with pytest.raises(
        Exception,
        match=r"You cannot set ‘layer_name’ and ‘layer_col’ at the same time. Please pick one or the other.",
    ):
        g.load_edge_props_from_pandas(
            edges_df, "src", "dst", layer="blah", layer_col="marbles"
        )

    with pytest.raises(
        Exception,
        match=r"You cannot set ‘layer_name’ and ‘layer_col’ at the same time. Please pick one or the other.",
    ):
        g.load_edge_deletions_from_pandas(
            edges_df, "time", "src", "dst", layer="blah", layer_col="marbles"
        )

    # CHECK IF JUST LAYER WORKS
    g = PersistentGraph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer="blah")
    assert g.edges.layer_names.collect() == [
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
        ["blah"],
    ]
    assert g.unique_layers == ["blah"]

    g = PersistentGraph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer="blah")
    g.load_edge_props_from_pandas(
        edges_df, "src", "dst", layer="blah", metadata=["marbles"]
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
        zip(g.layer("blah").edges.id, g.layer("blah").edges.metadata.get("marbles"))
    ) == {
        (1, 2): "red",
        (2, 3): "blue",
        (3, 4): "green",
        (4, 5): "yellow",
        (5, 6): "purple",
    }

    g = PersistentGraph()
    g.load_edge_deletions_from_pandas(edges_df, "time", "src", "dst", layer="blah")
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
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="marbles")
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
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="marbles")
    g.load_edge_props_from_pandas(
        edges_df, "src", "dst", layer_col="marbles", metadata=["marbles"]
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
    assert dict(zip(g.edges.id, g.edges.metadata.get("marbles"))) == {
        (1, 2): {"red": "red"},
        (2, 3): {"blue": "blue"},
        (3, 4): {"green": "green"},
        (4, 5): {"yellow": "yellow"},
        (5, 6): {"purple": "purple"},
    }

    g = PersistentGraph()
    g.load_edge_deletions_from_pandas(
        edges_df, "time", "src", "dst", layer_col="marbles"
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


def test_node_both_option_failures_pandas():
    nodes_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
            "node_type": ["P1", "P2", "P3", "P4", "P5", "P6"],
        }
    )
    # CHECK ALL NODE FUNCTIONS ON GRAPH FAIL WITH BOTH NODE_TYPE AND NODE_TYPE_COL
    with pytest.raises(
        Exception,
        match=r"You cannot set ‘node_type_name’ and ‘node_type_col’ at the same time. Please pick one or the other.",
    ):
        g = Graph()
        g.load_nodes_from_pandas(
            nodes_df, "time", "id", node_type="node_type", node_type_col="node_type"
        )

    with pytest.raises(
        Exception,
        match=r"You cannot set ‘node_type_name’ and ‘node_type_col’ at the same time. Please pick one or the other.",
    ):
        g = Graph()
        g.load_node_props_from_pandas(
            nodes_df, "id", node_type="node_type", node_type_col="node_type"
        )

    # CHECK IF JUST NODE_TYPE WORKS
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id", node_type="node_type")
    assert g.nodes.node_type == [
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
    ]
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(nodes_df, "id", node_type="node_type")
    assert g.nodes.node_type == [
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
        "node_type",
    ]

    # CHECK IF JUST NODE_TYPE_COL WORKS
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id", node_type_col="node_type")
    assert g.nodes.node_type == {1: "P1", 2: "P2", 3: "P3", 4: "P4", 5: "P5", 6: "P6"}
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(nodes_df, "id", node_type_col="node_type")
    assert g.nodes.node_type == {1: "P1", 2: "P2", 3: "P3", 4: "P4", 5: "P5", 6: "P6"}


def _ms_from_date(d: datetime.date) -> int:
    return int(
        datetime.datetime(
            d.year, d.month, d.day, tzinfo=datetime.timezone.utc
        ).timestamp()
        * 1000
    )


def _utc_midnight(dt: datetime.date) -> datetime.datetime:
    return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=datetime.timezone.utc)


def test_load_date32_from_pandas():
    arrow_date32_dtype = pd.ArrowDtype(pa.date32())

    dates = [
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 2),
        datetime.date(1970, 1, 3),
    ]
    df = pd.DataFrame(
        {
            "time": pd.array(dates, dtype=arrow_date32_dtype),
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
        }
    )
    # Ensure column is date32
    assert "date32[day]" in str(df["time"].dtype)
    g = Graph()
    g.load_edges_from_pandas(df, "time", "src", "dst")
    expected_edge_times = [_ms_from_date(d) for d in dates]

    # Start with pandas datetime64, then cast to Arrow date32 via astype()
    dates = [
        datetime.date(1970, 1, 4),
        datetime.date(1970, 1, 5),
        datetime.date(1970, 1, 6),
    ]
    ts = pd.to_datetime(["1970-01-04", "1970-01-05", "1970-01-06"]).astype(
        arrow_date32_dtype
    )
    df = pd.DataFrame(
        {
            "time": ts,
            "src": [10, 20, 30],
            "dst": [11, 21, 31],
        }
    )
    # Ensure column is date32
    assert "date32[day]" in str(df["time"].dtype)
    g.load_edges_from_pandas(df, "time", "src", "dst")
    expected_edge_times.extend([_ms_from_date(d) for d in dates])

    # Load edges datetime64
    dates = [
        datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 2, 12, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc),
    ]
    ts = pd.to_datetime(
        ["2020-01-01 00:00:00", "2020-01-02 12:00:00", "2020-01-03 23:59:59"]
    )
    df = pd.DataFrame({"time": ts, "src": [100, 200, 300], "dst": [200, 300, 400]})
    g.load_edges_from_pandas(df=df, time="time", src="src", dst="dst")
    expected_edge_times.extend([int(d.timestamp() * 1000) for d in dates])

    # Load nodes with arrowdtype
    dates = [
        datetime.date(1970, 1, 7),
        datetime.date(1970, 1, 8),
        datetime.date(1970, 1, 9),
    ]
    df = pd.DataFrame(
        {
            "id": [1000, 2000, 3000],
            "time": pd.array(dates, dtype=arrow_date32_dtype),
        }
    )
    assert "date32[day]" in str(df["time"].dtype)
    g.load_nodes_from_pandas(df, time="time", id="id")
    expected_node_datetimes = {
        1000: _utc_midnight(dates[0]),
        2000: _utc_midnight(dates[1]),
        3000: _utc_midnight(dates[2]),
    }

    # Load nodes with astype()
    dates = [
        datetime.date(1970, 1, 10),
        datetime.date(1970, 1, 11),
        datetime.date(1970, 1, 12),
    ]
    ts = pd.to_datetime(["1970-01-10", "1970-01-11", "1970-01-12"]).astype(
        arrow_date32_dtype
    )
    df = pd.DataFrame(
        {
            "id": [10_000, 20_000, 30_000],
            "time": ts,
        }
    )
    assert "date32[day]" in str(df["time"].dtype)
    g.load_nodes_from_pandas(df, time="time", id="id")
    expected_node_datetimes[10_000] = _utc_midnight(dates[0])
    expected_node_datetimes[20_000] = _utc_midnight(dates[1])
    expected_node_datetimes[30_000] = _utc_midnight(dates[2])

    # Load nodes datetime64
    ts = pd.to_datetime(
        ["2021-01-01 00:00:00", "2021-01-02 12:00:00", "2021-01-03 23:59:59"]
    )
    df = pd.DataFrame({"id": [100_000, 200_000, 300_000], "time": ts})
    g.load_nodes_from_pandas(df=df, time="time", id="id")
    expected_node_datetimes[100_000] = datetime.datetime(
        2021, 1, 1, tzinfo=datetime.timezone.utc
    )
    expected_node_datetimes[200_000] = datetime.datetime(
        2021, 1, 2, 12, 0, tzinfo=datetime.timezone.utc
    )
    expected_node_datetimes[300_000] = datetime.datetime(
        2021, 1, 3, 23, 59, 59, tzinfo=datetime.timezone.utc
    )

    # check equality
    actual_node_datetimes = {v.id: v.history.dt[0] for v in g.nodes}
    for id, dt in expected_node_datetimes.items():
        assert actual_node_datetimes[id] == dt

    actual_edge_times = sorted([e.time for e in g.edges.explode()])
    assert actual_edge_times == expected_edge_times


def test_load_date_dataframe():
    dates = [
        datetime.date(1970, 1, 1),
        datetime.date(1970, 1, 2),
        datetime.date(1970, 1, 3),
        datetime.date(1970, 1, 4),
    ]
    df = pd.DataFrame({
        "time": dates,
        "src": [1, 2, 3, 4],
        "dst": [5, 6, 7, 8],
    })
    g = Graph()
    g.load_edges_from_pandas(df=df, time="time", src="src", dst="dst")
    assert sorted(g.edges.history.flatten()) == ["1970-01-01 00:00:00", "1970-01-02 00:00:00", "1970-01-03 00:00:00", "1970-01-04 00:00:00"]

    g = Graph()
    g.load_nodes_from_pandas(df=df, time="time", id="src")
    assert sorted(g.nodes.history.flatten()) == ["1970-01-01 00:00:00", "1970-01-02 00:00:00", "1970-01-03 00:00:00", "1970-01-04 00:00:00"]


def test_load_edges_from_pandas_csv_c_engine_time_utf8(tmp_path):
    # test c engine
    csv_1 = """src,dst,time,value
1,2,2020-01-01T00:00:00Z,10
2,3,2020-01-02T00:00:00Z,20
3,4,2020-01-03T00:00:00Z,30
"""
    p = tmp_path / "edges.csv"
    p.write_text(csv_1)
    cols = ["src", "dst", "time", "value"]

    df = pd.read_csv(p, usecols=cols, engine="c")
    # Expect string/object dtype for time
    assert "string" in str(df["time"].dtype) or df["time"].dtype == object

    g = Graph()
    # this line should not raise an exception
    g.load_edges_from_pandas(
        df=df, time="time", src="src", dst="dst", properties=["value", "time"]
    )
    expected_edge_times = [
        datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 2, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 3, tzinfo=datetime.timezone.utc),
    ]
    expected_edge_time_props = [
        "2020-01-01T00:00:00Z",
        "2020-01-02T00:00:00Z",
        "2020-01-03T00:00:00Z",
    ]
    expected_edge_values = [10, 20, 30]

    # test pyarrow engine
    csv_2 = """src,dst,time_dt,value
10,20,2020-01-04T00:00:00Z,100
20,30,2020-01-05T00:00:00Z,200
30,40,2020-01-06T00:00:00Z,300
"""
    p = tmp_path / "edges_2.csv"
    p.write_text(csv_2)
    df = pd.read_csv(
        p,
        usecols=["src", "dst", "time_dt", "value"],
        engine="pyarrow",
        dtype_backend="pyarrow",
    )
    # ensure it’s datetime
    if "datetime" not in str(df["time_dt"].dtype):
        df["time_dt"] = pd.to_datetime(df["time_dt"], utc=True)
    g.load_edges_from_pandas(
        df=df, time="time_dt", src="src", dst="dst", properties=["value", "time_dt"]
    )
    expected_time_dt = [
        datetime.datetime(2020, 1, 4, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 5, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 1, 6, tzinfo=datetime.timezone.utc),
    ]
    # check time_dt separately because it's a datetime, not str
    assert (
        sorted(e.properties["time_dt"] for e in g.edges if "time_dt" in e.properties)
        == expected_time_dt
    )
    expected_edge_times.extend(expected_time_dt)
    expected_edge_values.extend([100, 200, 300])

    # load nodes with c engine
    csv_3 = """id,time
    100,2020-01-07T00:00:00Z
    200,2020-01-08T00:00:00Z
    300,2020-01-09T00:00:00Z
    """
    p = tmp_path / "nodes.csv"
    p.write_text(csv_3)
    cols = ["id", "time"]
    df = pd.read_csv(p, usecols=cols, engine="c")
    assert "string" in str(df["time"].dtype) or df["time"].dtype == object
    g.load_nodes_from_pandas(df=df, time="time", id="id", properties=["time"])
    expected_node_times = {
        100: datetime.datetime(2020, 1, 7, tzinfo=datetime.timezone.utc),
        200: datetime.datetime(2020, 1, 8, tzinfo=datetime.timezone.utc),
        300: datetime.datetime(2020, 1, 9, tzinfo=datetime.timezone.utc),
    }
    # ingested as strings from csv for props
    expected_node_time_props = {
        100: "2020-01-07T00:00:00Z",
        200: "2020-01-08T00:00:00Z",
        300: "2020-01-09T00:00:00Z",
    }

    # Load edges with various time formats
    # Mixed formats supported by TryIntoTime: RFC3339, RFC2822, date-only, with/without millis, space ' ' or 'T'
    csv_mixed = """src,dst,time,value
1000,2000,2021-01-01T00:00:00Z,1000
2000,3000,"Sat, 02 Jan 2021 13:00:00 GMT",2000
3000,4000,2021-01-03,3000
4000,5000,2021-01-04T12:34:56.789,4000
5000,6000,2021-01-05 23:59:59.000,5000
6000,7000,2021-01-06T00:00:00,6000
"""
    p = tmp_path / "edges_mixed.csv"
    p.write_text(csv_mixed)
    cols = ["src", "dst", "time", "value"]
    df = pd.read_csv(p, usecols=cols, engine="c")
    assert "string" in str(df["time"].dtype) or df["time"].dtype == object
    g.load_edges_from_pandas(
        df=df, time="time", src="src", dst="dst", properties=["value", "time"]
    )
    expected_edge_times.extend(
        [
            datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 1, 2, 13, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 1, 3, tzinfo=datetime.timezone.utc),
            datetime.datetime(
                2021, 1, 4, 12, 34, 56, 789000, tzinfo=datetime.timezone.utc
            ),
            datetime.datetime(2021, 1, 5, 23, 59, 59, tzinfo=datetime.timezone.utc),
            datetime.datetime(2021, 1, 6, tzinfo=datetime.timezone.utc),
        ]
    )
    expected_edge_time_props.extend(
        [
            "2021-01-01T00:00:00Z",
            "2021-01-03",
            "2021-01-04T12:34:56.789",
            "2021-01-05 23:59:59.000",
            "2021-01-06T00:00:00",
            "Sat, 02 Jan 2021 13:00:00 GMT",
        ]
    )
    expected_edge_values.extend([1000, 2000, 3000, 4000, 5000, 6000])

    # Load nodes with various time formats
    csv_mixed_2 = """id,time
10000,2022-01-01T00:00:00Z
20000,"Sun, 02 Jan 2022 13:00:00 GMT"
30000,2022-01-03
40000,2022-01-04T12:34:56.789
50000,2022-01-05 23:59:59.000
60000,2022-01-06T00:00:00
"""
    p = tmp_path / "nodes_mixed.csv"
    p.write_text(csv_mixed_2)
    cols = ["id", "time"]
    df = pd.read_csv(p, usecols=cols, engine="c")
    assert "string" in str(df["time"].dtype) or df["time"].dtype == object
    g.load_nodes_from_pandas(df=df, time="time", id="id", properties=["time"])
    expected_node_times[10_000] = datetime.datetime(
        2022, 1, 1, tzinfo=datetime.timezone.utc
    )
    expected_node_times[20_000] = datetime.datetime(
        2022, 1, 2, 13, 0, 0, tzinfo=datetime.timezone.utc
    )
    expected_node_times[30_000] = datetime.datetime(
        2022, 1, 3, tzinfo=datetime.timezone.utc
    )
    expected_node_times[40_000] = datetime.datetime(
        2022, 1, 4, 12, 34, 56, 789000, tzinfo=datetime.timezone.utc
    )
    expected_node_times[50_000] = datetime.datetime(
        2022, 1, 5, 23, 59, 59, tzinfo=datetime.timezone.utc
    )
    expected_node_times[60_000] = datetime.datetime(
        2022, 1, 6, tzinfo=datetime.timezone.utc
    )
    expected_node_time_props[10_000] = "2022-01-01T00:00:00Z"
    expected_node_time_props[20_000] = "Sun, 02 Jan 2022 13:00:00 GMT"
    expected_node_time_props[30_000] = "2022-01-03"
    expected_node_time_props[40_000] = "2022-01-04T12:34:56.789"
    expected_node_time_props[50_000] = "2022-01-05 23:59:59.000"
    expected_node_time_props[60_000] = "2022-01-06T00:00:00"

    # check edges
    assert sorted(e.history.dt[0] for e in g.edges) == expected_edge_times
    assert (
        sorted(e.properties["time"] for e in g.edges if "time" in e.properties)
        == expected_edge_time_props
    )
    assert sorted(e.properties["value"] for e in g.edges) == expected_edge_values

    # check nodes
    actual_node_times = {v.id: v.history.dt[0] for v in g.nodes}
    for id, dt in expected_node_times.items():
        assert actual_node_times[id] == dt
    assert {
        v.id: v.properties["time"] for v in g.nodes if "time" in v.properties
    } == expected_node_time_props
