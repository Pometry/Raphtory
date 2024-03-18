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
        
    g = Graph.load_from_pandas(df, "src", "dst", "time", ["weight", "marbles"])
    assertions(g)
    
    g = GraphWithDeletions.load_from_pandas(df, "src", "dst", "time", ["weight", "marbles"])
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
        g = Graph.load_from_pandas(df, "src", "dst", "time", ["weight", "marbles"])
    assertions(exc_info)
    
    with pytest.raises(Exception) as exc_info:
        g = GraphWithDeletions.load_from_pandas(df, "src", "dst", "time", ["weight", "marbles"])
    assertions(exc_info)


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
    g.load_nodes_from_pandas(nodes_df, "id", "time", "node_type", properties=["name"])
    g.load_edges_from_pandas(edges_df, "src", "dst", "time", ["weight", "marbles"])
    assertions(g)
    
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(nodes_df, "id", "time", "node_type", properties=["name"])
    g.load_edges_from_pandas(edges_df, "src", "dst", "time", ["weight", "marbles"])
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

    g = Graph.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_type="node_type",
    )
    assertions(g)
    
    g = GraphWithDeletions.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_type="node_type",
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
        "id",
        "time",
        "node_type",
        properties=["name"],
        shared_const_properties={"tag": "test_tag"},
    )
    assertions1(g)
    
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(
        nodes_df,
        "id",
        "time",
        "node_type",
        properties=["name"],
        shared_const_properties={"tag": "test_tag"},
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
        nodes_df, "id", "time", "node_type", properties=["name"], const_properties=["type"]
    )
    assertions2(g)
    
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(
        nodes_df, "id", "time", "node_type", properties=["name"], const_properties=["type"]
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
        "src",
        "dst",
        "time",
        properties=["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
        layer_in_df=False,
    )
    assertions3(g)
    
    g = GraphWithDeletions()
    g.load_edges_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        properties=["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="test_layer",
        layer_in_df=False,
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
        edges_df, "src", "dst", "time", ["weight", "marbles"], layer="layers"
    )
    assertions4(g)
    
    g = GraphWithDeletions()
    g.load_edges_from_pandas(
        edges_df, "src", "dst", "time", ["weight", "marbles"], layer="layers"
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

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="test_layer",
        layer_in_df=False,
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_const_properties={"type": "Person"},
    )
    assertions5(g)
    
    g = GraphWithDeletions.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="test_layer",
        layer_in_df=False,
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_shared_const_properties={"type": "Person"},
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

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="layers",
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_const_properties=["type"],
    )
    assertions6(g)
    
    g = GraphWithDeletions.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        edge_layer="layers",
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        node_const_properties=["type"],
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
    
    g = Graph.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        edge_layer="layers",
    )
    g.load_node_props_from_pandas(
        nodes_df, "id", const_properties=["type"], shared_const_properties={"tag": "test_tag"}
    )
    assertions7(g)
    
    def assertions8(g):
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

    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        const_properties=["marbles_const"],
        shared_const_properties={"tag": "test_tag"},
        layer="layers",
    )
    assertions8(g)
    
    g = GraphWithDeletions.load_from_pandas(
        edges_df,
        edge_src="src",
        edge_dst="dst",
        edge_time="time",
        edge_properties=["weight", "marbles"],
        node_df=nodes_df,
        node_id="id",
        node_time="time",
        node_properties=["name"],
        edge_layer="layers",
    )
    g.load_node_props_from_pandas(
        nodes_df, "id", const_properties=["type"], shared_const_properties={"tag": "test_tag"}
    )
    assertions7(g)
    
    g.load_edge_props_from_pandas(
        edges_df,
        "src",
        "dst",
        const_properties=["marbles_const"],
        shared_const_properties={"tag": "test_tag"},
        layer="layers",
    )
    assertions8(g)
    
    def assertions_layers_in_df(g):
        assert g.unique_layers == ["_default", "layer 1", "layer 2", "layer 3", "layer 4", "layer 5"]
        assert g.layers(["layer 1"]).edges.src.id.collect() == [1]
        assert g.layers(["layer 3"]).edges.src.id.collect() == [3]
        with pytest.raises(
            Exception,
            match=re.escape(
                "Invalid layer test_layer."
            ),
        ):
            g.layers(["test_layer"])
            
    g = Graph()
    g.load_edges_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        ["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="layers",
        layer_in_df=True,
    )
    assertions_layers_in_df(g)
    
    g = GraphWithDeletions()
    g.load_edges_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        ["weight", "marbles"],
        const_properties=["marbles_const"],
        shared_const_properties={"type": "Edge", "tag": "test_tag"},
        layer="layers",
        layer_in_df=True,
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
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="not_src",
            edge_dst="not_dst",
            edge_time="not_time",
        )
        
    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_src, not_dst, not_time"
        ),
    ):
        g = GraphWithDeletions.load_from_pandas(
            edges_df,
            edge_src="not_src",
            edge_dst="not_dst",
            edge_time="not_time",
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_weight, bleep_bloop"
        ),
    ):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["not_weight", "marbles"],
            edge_const_properties=["bleep_bloop"],
            node_df=nodes_df,
            node_id="id",
            node_time="time",
            node_properties=["name"],
        )
        
    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_weight, bleep_bloop"
        ),
    ):
        g = GraphWithDeletions.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["not_weight", "marbles"],
            edge_const_properties=["bleep_bloop"],
            node_df=nodes_df,
            node_id="id",
            node_time="time",
            node_properties=["name"],
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_id, not_time, not_name"
        ),
    ):
        g = Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["weight", "marbles"],
            node_df=nodes_df,
            node_id="not_id",
            node_time="not_time",
            node_properties=["not_name"],
        )
    
    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: not_id, not_time, not_name"
        ),
    ):
        g = GraphWithDeletions.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["weight", "marbles"],
            node_df=nodes_df,
            node_id="not_id",
            node_time="not_time",
            node_properties=["not_name"],
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
            const_properties=["wait", "marples"],
        )
    
    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, dist, wait, marples"
        ),
    ):
        g = GraphWithDeletions()
        g.load_edge_props_from_pandas(
            edges_df,
            src="sauce",
            dst="dist",
            const_properties=["wait", "marples"],
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
            const_properties=["wait", "marples"],
        )
        
    with pytest.raises(
        Exception,
        match=re.escape(
            "columns are not present within the dataframe: sauce, wait, marples"
        ),
    ):
        g = GraphWithDeletions()
        g.load_node_props_from_pandas(
            nodes_df,
            id="sauce",
            const_properties=["wait", "marples"],
        )


def test_none_columns_edges():
    edges_df = pd.DataFrame(
        {"src": [1, None, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        Graph.load_from_pandas(edges_df, "src", "dst", "time")
        
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        GraphWithDeletions.load_from_pandas(edges_df, "src", "dst", "time")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, None, 6], "time": [1, 2, 3, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        Graph.load_from_pandas(edges_df, "src", "dst", "time")
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        GraphWithDeletions.load_from_pandas(edges_df, "src", "dst", "time")

    edges_df = pd.DataFrame(
        {"src": [1, 2, 3, 4, 5], "dst": [2, 3, 4, 5, 6], "time": [1, 2, None, 4, 5]}
    )
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        Graph.load_from_pandas(edges_df, "src", "dst", "time")
    with pytest.raises(
        Exception, match=re.escape("Ensure these contain no NaN, Null or None values.")
    ):
        GraphWithDeletions.load_from_pandas(edges_df, "src", "dst", "time")


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
        Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["weight"],
        )
    with pytest.raises(
        Exception,
        match=re.escape(
            """"Could not convert '2.0' with type str: tried to convert to double", 'Conversion failed for column weight with type object'"""
        ),
    ):
        GraphWithDeletions.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["weight"],
        )

    with pytest.raises(
        Exception,
        match=re.escape(
            "Column marbles could not be parsed -  must be either u64, i64, f64, f32, bool or string. Ensure it contains no NaN, Null or None values."
        ),
    ):
        Graph.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["marbles"],
        )
    with pytest.raises(
        Exception,
        match=re.escape(
            "Column marbles could not be parsed -  must be either u64, i64, f64, f32, bool or string. Ensure it contains no NaN, Null or None values."
        ),
    ):
        GraphWithDeletions.load_from_pandas(
            edges_df,
            edge_src="src",
            edge_dst="dst",
            edge_time="time",
            edge_properties=["marbles"],
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
        assert g.get_all_node_types() == ['node_type']
        assert g.count_nodes() == 4
    
    def nodes_assertions3(g):
        all_node_types = g.get_all_node_types()
        all_node_types.sort()
        assert all_node_types == ["node_type1", "node_type2", "node_type3", "node_type4"]
        assert g.count_nodes() == 4
    
    def edges_assertions(g):
        assert g.get_all_node_types() == []
        assert g.count_nodes() == 6
    
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "id", "time")
    nodes_assertions(g)
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(nodes_df, "id", "time")
    nodes_assertions(g)
    
    g = Graph()
    g.load_nodes_from_pandas(nodes_df, "id", "time", node_type="node_type", node_type_in_df=False)
    nodes_assertions2(g)
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(nodes_df, "id", "time", node_type="node_type", node_type_in_df=False)
    nodes_assertions2(g)
    
    g = Graph()
    g.load_nodes_from_pandas(nodes_df2, "id", "time", node_type="node_type", node_type_in_df=False)
    nodes_assertions2(g)
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(nodes_df2, "id", "time", node_type="node_type", node_type_in_df=False)
    nodes_assertions2(g)
    
    g = Graph()
    g.load_nodes_from_pandas(nodes_df2, "id", "time", node_type="node_type", node_type_in_df=True)
    nodes_assertions3(g)
    g = GraphWithDeletions()
    g.load_nodes_from_pandas(nodes_df2, "id", "time", node_type="node_type", node_type_in_df=True)
    nodes_assertions3(g)
    
    g = Graph.load_from_pandas(edges_df, "src", "dst", "time")
    edges_assertions(g)
    g = Graph.load_from_pandas(edges_df, "src", "dst", "time")
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
    
    g = GraphWithDeletions()
    g.load_edges_from_pandas(edges_df, "src", "dst", "time")
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 3, 4, 5]
    g.load_edges_deletions_from_pandas(edge_dels_df, "src", "dst", "time")
    assert g.window(10, 12).edges.src.id.collect() == [1, 2, 5]

