import math
import re
import sys

import numpy
import pandas as pd
import pandas.core.frame
import pytest
from raphtory import Graph, PersistentGraph, PyDirection
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

        assert g.nodes.id.collect() == expected_nodes
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
        assert "Failed to load graph" in str(exc_info.value)
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
    assert "Failed to load graph" in str(exc_info.value)
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
        nodes = []
        for v in g.nodes:
            name = v["name"]
            nodes.append((v.id, name))

        assert g.nodes.id.collect() == expected_node_ids
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

        nodes = []
        for v in g.nodes:
            name = v["name"]
            nodes.append((v.id, name))

        assert nodes == expected_nodes
        assert g.nodes.id.collect() == expected_node_ids
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
        assert g.nodes.properties.constant.get("tag").collect() == [
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
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions1(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(
        nodes_df,
        "time",
        "id",
        node_type_col="node_type",
        properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions1(g)

    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        node_type_col="node_type",
        constant_properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions1(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(nodes_df, "time", "id")
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        node_type_col="node_type",
        constant_properties=["name"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions1(g)

    def assertions2(g):
        assert g.nodes.properties.constant.get("type").collect() == [
            "Person 1",
            "Person 2",
            "Person 3",
            "Person 4",
            "Person 5",
            "Person 6",
        ]

    g = Graph()
    g.load_nodes_from_pandas(
        nodes_df,
        "time",
        "id",
        node_type_col="node_type",
        properties=["name"],
        constant_properties=["type"],
    )
    assertions2(g)

    g = PersistentGraph()
    g.load_nodes_from_pandas(
        nodes_df,
        "id",
        "time",
        node_type_col="node_type",
        properties=["name"],
        constant_properties=["type"],
    )
    assertions2(g)

    def assertions3(g):
        assert g.unique_layers == ["_default", "test_layer"]
        assert g.layers(["test_layer"]).edges.src.id.collect() == [1, 2, 3, 4, 5]
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
        assert g.edges.properties.constant.get("marbles_const").collect() == [
            {"test_layer": "red"},
            {"test_layer": "blue"},
            {"test_layer": "green"},
            {"test_layer": "yellow"},
            {"test_layer": "purple"},
        ]

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        properties=["weight", "marbles"],
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
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
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
    )
    assertions3(g)

    def assertions4(g):
        assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
        assert g.layers(["layer 1", "layer 2"]).edges.src.id.collect() == [1, 2]
        assert g.layers(["layer 1", "layer 2", "layer 3"]).edges.src.id.collect() == [
            1,
            2,
            3,
        ]
        assert g.layers(["layer 1", "layer 4", "layer 5"]).edges.src.id.collect() == [
            1,
            4,
            5,
        ]

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
        assert g.nodes.properties.constant.get("type").collect() == [
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
            "Person",
        ]
        assert g.layers(["test_layer"]).edges.src.id.collect() == [1, 2, 3, 4, 5]

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
        shared_constant_properties={"type": "Person"},
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
        shared_constant_properties={"type": "Person"},
    )
    assertions5(g)

    def assertions6(g):
        assert g.nodes.properties.constant.get("type").collect() == [
            "Person 1",
            "Person 2",
            "Person 3",
            "Person 4",
            "Person 5",
            "Person 6",
        ]
        assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
        assert g.layers(["layer 1", "layer 2"]).edges.src.id.collect() == [1, 2]
        assert g.layers(["layer 1", "layer 2", "layer 3"]).edges.src.id.collect() == [
            1,
            2,
            3,
        ]
        assert g.layers(["layer 1", "layer 4", "layer 5"]).edges.src.id.collect() == [
            1,
            4,
            5,
        ]

    g = Graph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="layers")
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        constant_properties=["type"],
    )
    assertions6(g)

    g = PersistentGraph()
    g.load_edges_from_pandas(edges_df, "time", "src", "dst", layer_col="layers")
    g.load_nodes_from_pandas(
        df=nodes_df,
        time="time",
        id="id",
        properties=["name"],
        constant_properties=["type"],
    )
    assertions6(g)

    def assertions7(g):
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

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
        layer_col="layers",
    )
    g.load_nodes_from_pandas(
        df=nodes_df, time="time", id="id", properties=["name"]
    )
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        constant_properties=["type"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions7(g)

    def assertions8(g):
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

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        constant_properties=["marbles_const"],
        shared_constant_properties={"tag": "test_tag"},
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
    g.load_nodes_from_pandas(
        df=nodes_df, time="time", id="id", properties=["name"]
    )
    g.load_node_props_from_pandas(
        nodes_df,
        "id",
        constant_properties=["type"],
        shared_constant_properties={"tag": "test_tag"},
    )
    assertions7(g)

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        constant_properties=["marbles_const"],
        shared_constant_properties={"tag": "test_tag"},
        layer_col="layers",
    )
    assertions8(g)

    def assertions_layers_in_df(g):
        assert g.unique_layers == [
            "_default",
            "layer 1",
            "layer 2",
            "layer 3",
            "layer 4",
            "layer 5",
        ]
        assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
        assert g.layers(["layer 3"]).edges.src.id.collect() == [3]
        with pytest.raises(
            Exception,
            match=re.escape(
                "Invalid layer: test_layer. Valid layers: _default, layer 1, layer 2, layer 3, layer 4, layer 5"
            ),
        ):
            g.layers(["test_layer"])

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "time",
        "src",
        "dst",
        ["weight", "marbles"],
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
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
        constant_properties=["marbles_const"],
        shared_constant_properties={"type": "Edge", "tag": "test_tag"},
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
            constant_properties=["bleep_bloop"],
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
            constant_properties=["bleep_bloop"],
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
            constant_properties=["wait", "marples"],
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
            constant_properties=["wait", "marples"],
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
            constant_properties=["wait", "marples"],
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
            constant_properties=["wait", "marples"],
        )


def test_none_columns_edges():
    edges_df = pd.DataFrame(
        {"src": [1, None, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        g = Graph()
        g.load_edges_from_pandas(edges_df, "time", "src", "dst")

    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        PersistentGraph().load_edges_from_pandas(edges_df, "time", "src", "dst")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, None, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        Graph().load_edges_from_pandas(edges_df, "time", "src", "dst")
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        PersistentGraph().load_edges_from_pandas(edges_df, "time", "src", "dst")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, None, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        Graph().load_edges_from_pandas(edges_df, "time", "src", "dst")
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
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
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 3, 4, 5]
    g.load_edges_deletions_from_pandas(edge_dels_df, "time", "src", "dst")
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 5]
