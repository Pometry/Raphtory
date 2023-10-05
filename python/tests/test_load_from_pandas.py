import math
import re
import sys

import numpy
import pandas as pd
import pandas.core.frame
import pytest
from raphtory import Graph, GraphWithDeletions, PyDirection
from raphtory import algorithms
from raphtory import graph_loader
import tempfile
from math import isclose
import datetime


def test_load_from_pandas():
    import pandas as pd

    df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )

    g = Graph.load_from_pandas(df, "src", "dst", "time", ["weight", "marbles"])

    assert g.vertices.id.collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))

    assert edges == [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]


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

    vertices_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
        }
    )

    g = Graph()

    g.load_vertices_from_pandas(vertices_df, "id", "time", ["name"])

    g.load_edges_from_pandas(edges_df, "src", "dst", "time", ["weight", "marbles"])

    assert g.vertices.id.collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))

    assert edges == [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    vertices = []
    for v in g.vertices:
        name = v["name"]
        vertices.append((v.id, name))

    assert vertices == [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]


def test_load_from_pandas_vertices():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, 2.0, 3.0, 4.0, 5.0],
            "marbles": ["red", "blue", "green", "yellow", "purple"],
        }
    )

    vertices_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
        }
    )

    g = Graph.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_props=["weight", "marbles"],
        vertex_df=vertices_df,
        vertex_id="id",
        vertex_time="time",
        vertex_props=["name"],
    )

    assert g.vertices.id.collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges:
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src.id, e.dst.id, weight, marbles))

    assert edges == [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    vertices = []
    for v in g.vertices:
        name = v["name"]
        vertices.append((v.id, name))

    assert vertices == [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]


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
    vertices_df = pd.DataFrame(
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
        }
    )
    g = Graph()
    g.load_vertices_from_pandas(
        vertices_df,
        "id",
        "time",
        ["name"],
        shared_const_props={"type": "Person", "tag": "test_tag"},
    )
    assert g.vertices.properties.constant.get("type").collect() == [
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
    ]
    assert g.vertices.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]

    g = Graph()
    g.load_vertices_from_pandas(
        vertices_df, "id", "time", ["name"], const_props=["type"]
    )
    assert g.vertices.properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]

    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        ["weight", "marbles"],
        const_props=["marbles_const"],
        shared_const_props={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
        layer_in_df=False
    )

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
        edges_df, "src", "dst", "time", ["weight", "marbles"], layer="layers"
    )
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

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="test_layer",
        layer_in_df=False,
        vertex_df=vertices_df,
        vertex_id="id",
        vertex_time="time",
        vertex_props=["name"],
        vertex_shared_const_props={"type": "Person"},
    )
    assert g.vertices.properties.constant.get("type").collect() == [
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
    ]
    assert g.layers(["test_layer"]).edges.src.id.collect() == [1, 2, 3, 4, 5]

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="layers",
        vertex_df=vertices_df,
        vertex_id="id",
        vertex_time="time",
        vertex_props=["name"],
        vertex_const_props=["type"],
    )
    assert g.vertices.properties.constant.get("type").collect() == [
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

    g = Graph.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_props=["weight", "marbles"],
        vertex_df=vertices_df,
        vertex_id="id",
        vertex_time="time",
        vertex_props=["name"],
        edge_layer="layers",
    )

    g.load_vertex_props_from_pandas(
        vertices_df, "id", const_props=["type"], shared_const_props={"tag": "test_tag"}
    )
    assert g.vertices.properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]
    assert g.vertices.properties.constant.get("tag").collect() == [
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
        "test_tag",
    ]

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        const_props=["marbles_const"],
        shared_const_props={"tag": "test_tag"},
        layer="layers",
    )
    assert g.layers(["layer 1", "layer 2", "layer 3"]).edges.properties.constant.get(
        "marbles_const"
    ).collect() == [{"layer 1": "red"}, {"layer 2": "blue"}, {"layer 3": "green"}]
    assert g.edges.properties.constant.get("tag").collect() == [
        {"layer 1": "test_tag"},
        {"layer 2": "test_tag"},
        {"layer 3": "test_tag"},
        {"layer 4": "test_tag"},
        {"layer 5": "test_tag"},
    ]

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

    vertices_df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"],
            "time": [1, 2, 3, 4, 5, 6],
        }
    )

    with pytest.raises(Exception, match=re.escape('ColumnDoesNotExist("not_src, not_dst, not_time")')):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="not_src",
            edge_dst="not_dst",
            edge_time="not_time",
        )

    with pytest.raises(Exception, match=re.escape('ColumnDoesNotExist("not_weight, bleep_bloop")')):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_props=["not_weight", "marbles"],
            edge_const_props=["bleep_bloop"],
            vertex_df=vertices_df,
            vertex_id="id",
            vertex_time="time",
            vertex_props=["name"],
        )

    with pytest.raises(Exception, match=re.escape('ColumnDoesNotExist("not_id, not_time, not_name")')):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_props=["weight", "marbles"],
            vertex_df=vertices_df,
            vertex_id="not_id",
            vertex_time="not_time",
            vertex_props=["not_name"],
        )

    with pytest.raises(Exception, match=re.escape('ColumnDoesNotExist("sauce, dist, wait, marples")')):
        g = Graph()
        g.load_edge_props_from_pandas(
            edges_df,
            src="sauce",
            dst="dist",
            const_props=["wait", "marples"],
        )

    with pytest.raises(Exception, match=re.escape('ColumnDoesNotExist("sauce, wait, marples")')):
        g = Graph()
        g.load_vertex_props_from_pandas(
            vertices_df,
            id="sauce",
            const_props=["wait", "marples"],
        )


def test_none_columns_edges():
    edges_df = pd.DataFrame(
        {
            "src": [1, None, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(Exception, match=re.escape('Ensure these contain no NaN, Null or None values.')):
        Graph.load_from_pandas(
            edges_df,
            "src",
            "dst",
            "time"
        )

    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, None, 6],
            "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(Exception, match=re.escape('Ensure these contain no NaN, Null or None values.')):
        Graph.load_from_pandas(
            edges_df,
            "src",
            "dst",
            "time"
        )

    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, None, 4, 5]}
    )
    with pytest.raises(Exception, match=re.escape('Ensure these contain no NaN, Null or None values.')):
        Graph.load_from_pandas(
            edges_df,
            "src",
            "dst",
            "time"
        )

def test_unparsable_props():
    edges_df = pd.DataFrame(
        {
            "src": [1, 2, 3, 4, 5],
            "dst": [2, 3, 4, 5, 6],
            "time": [1, 2, 3, 4, 5],
            "weight": [1.0, "2.0", 3.0, 4.0, 5.0],
            "marbles": [{"red","red"}, ["blue"], ["green"], ["yellow"], ["purple"]],
        }
    )

    with pytest.raises(Exception, match=re.escape(""""Could not convert '2.0' with type str: tried to convert to double", 'Conversion failed for column weight with type object'""")):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_props=["weight"],
        )

    #with pytest.raises(Exception, match=re.escape(""""Could not convert '2.0' with type str: tried to convert to double", 'Conversion failed for column weight with type object'""")):
    g = Graph.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_props=["marbles"],
    )
    assert g.edges.collect==[]