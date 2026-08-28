import math
import sys
from email.policy import strict

import pandas as pd
import pandas.core.frame
import pytest
from raphtory import Graph, PersistentGraph
from raphtory import algorithms
from raphtory import graph_loader
import tempfile
from math import isclose
import datetime


def test_pyprophistvaluelist():
    g = Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 1),
        ("1", "4", 20, 2),
        ("2", "3", 5, 3),
        ("3", "2", 2, 4),
        ("3", "1", 1, 5),
        ("4", "3", 10, 6),
        ("4", "1", 5, 7),
        ("1", "5", 2, 8),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})

    v = g.node("1")
    res = sorted(v.out_edges.properties.temporal.get("value_dec").values().sum())
    assert res == [2, 20, 20]

    res = sorted(v.out_edges.properties.temporal.get("value_dec").values().count())
    assert res == [1, 1, 2]

    res = v.out_edges.properties.temporal.get("value_dec").values().sum().sum()
    assert res == 42

    res = v.out_edges.properties.temporal.get("value_dec").values().count().sum()
    assert res == 4

    g = Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 2),
        ("1", "2", 100, 3),
        ("1", "4", 20, 2),
        ("2", "3", 5, 3),
        ("3", "2", 2, 4),
        ("3", "1", 1, 5),
        ("4", "3", 10, 6),
        ("4", "1", 5, 7),
        ("1", "5", 2, 8),
        ("1", "5", 1, 9),
        ("1", "5", 5, 10),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})
    v = g.node("1")
    res = v.out_edges.properties.temporal.get(
        "value_dec"
    ).values()  # PyPropHistValueList([[10, 10, 100], [20], [2, 1, 5]])
    nbrs = v.out_edges.nbr.id
    assert dict(zip(nbrs, res.sum(), strict=True)) == {"2": 120, "4": 20, "5": 8}
    assert dict(zip(nbrs, res.min(), strict=True)) == {"2": 10, "4": 20, "5": 1}
    assert dict(zip(nbrs, res.max(), strict=True)) == {"2": 100, "4": 20, "5": 5}
    assert dict(zip(nbrs, res.count(), strict=True)) == {"2": 3, "4": 1, "5": 3}
    assert dict(zip(nbrs, res.median(), strict=True)) == {"2": 10, "4": 20, "5": 2}
    assert dict(zip(nbrs, res.mean(), strict=True)) == {"2": 40, "4": 20, "5": 8 / 3}
    assert dict(zip(nbrs, res.average(), strict=True)) == {"2": 40, "4": 20, "5": 8 / 3}


def test_empty_lists():
    # This checks that empty lists are handled correctly on all python property types
    g = Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 1),
        ("1", "4", 20, 2),
        ("1", "5", 2, 8),
        ("2", "3", 5, 3),
        ("3", "1", 1, 5),
        ("3", "2", 2, 4),
        ("4", "1", 5, 7),
        ("4", "3", 10, 6),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})

    props = g.nodes.out_edges.properties.temporal.get("value_dec").values()
    print(props.median())
    print(props.median().median())
    print(props.median().median().median())

    assert (
        g.nodes.out_edges.properties.temporal.get("value_dec")
        .values()
        .median()
        .median()
        .median()
        == 6.25  # median interpolates
    )
    assert (
        int(
            g.nodes.out_edges.properties.temporal.get("value_dec")
            .values()
            .mean()
            .mean()
            .mean()
            * 100
        )
        == 616
    )


def test_propiterable():
    import raphtory

    g = raphtory.Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 1),
        ("1", "2", 10, 1),
        ("1", "4", 20, 2),
        ("2", "3", 5, 3),
        ("3", "2", 2, 4),
        ("3", "1", 1, 5),
        ("4", "3", 10, 6),
        ("4", "1", 5, 7),
        ("1", "5", 2, 8),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})

    v = g.node("1")
    result = v.out_edges.properties.temporal.get("value_dec").values().flatten()
    assert sorted(result) == [2, 10, 10, 10, 20]
    assert result.sum() == 52
    assert result.median() == 10
    assert result.mean() == 10.4
    assert result.average() == 10.4
    assert result.min() == 2
    assert result.max() == 20
    assert result.count() == 5

    assert v.out_edges.properties.get("value_dec").sum() == 32
    assert v.out_edges.properties.get("value_dec").median() == 10

    total = g.nodes.in_edges.properties.get("value_dec").sum()
    assert dict(zip(g.nodes.id, total)) == {"1": 6, "2": 12, "3": 15, "4": 20, "5": 2}

    total = g.nodes.edges.properties.get("value_dec").sum()
    assert dict(zip(g.nodes.id, total)) == {"1": 38, "2": 17, "3": 18, "4": 35, "5": 2}

    total = dict(zip(g.nodes.id, g.nodes.out_edges.properties.get("value_dec").sum()))
    assert total == {"1": 32, "2": 5, "3": 3, "4": 15, "5": None}

    total = g.nodes.out_edges.properties.get("value_dec").sum().sum()
    assert total == 55

    total = g.nodes.out_edges.properties.get("value_dec").sum().median()
    assert total == 10

    total = g.nodes.out_edges.properties.get("value_dec").sum().drop_none()
    assert sorted(total) == [3, 5, 15, 32]

    total = g.nodes.out_edges.properties.get("value_dec").median()
    assert dict(zip(g.nodes.id, total)) == {
        "1": 10,
        "2": 5,
        "3": 1.5,
        "4": 7.5,
        "5": None,
    }

    total = g.node("1").in_edges.properties.get("value_dec").sum()
    assert total == 6

    total = g.node("1").in_edges.properties.get("value_dec").median()
    assert total == 3


def test_pypropvalue_list_listlist():
    g = Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 2),
        ("1", "2", 100, 3),
        ("1", "4", 20, 2),
        ("2", "3", 5, 3),
        ("3", "2", 2, 4),
        ("3", "1", 1, 5),
        ("4", "3", 10, 6),
        ("4", "1", 5, 7),
        ("1", "5", 2, 8),
        ("1", "5", 1, 9),
        ("1", "5", 5, 10),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})
    v = g.node("1")
    res = g.edges.properties.get(
        "value_dec"
    )  # PyPropValueList([100, 20, 5, 5, 5, 10, 1, 2])
    res_v = v.edges.properties.get("value_dec")  # PyPropValueList([100, 5, 20, 1, 5])
    res_ll = g.nodes.edges.properties.get("value_dec")
    nodes = g.nodes.id.collect()

    assert res.sum() == 148
    assert res_v.sum() == 131
    assert dict(zip(nodes, res_ll.sum())) == {
        "1": 131,
        "2": 107,
        "3": 18,
        "4": 35,
        "5": 5,
    }

    assert res.median() == 5
    assert res_v.median() == 5
    assert dict(zip(nodes, res_ll.median())) == {
        "1": 5.0,
        "2": 5.0,
        "3": 3.5,
        "4": 10.0,
        "5": 5.0,
    }

    assert res.min() == 1
    assert res_v.min() == 1
    assert dict(zip(nodes, res_ll.min())) == {"1": 1, "2": 2, "3": 1, "4": 5, "5": 5}

    assert res.max() == 100
    assert res_v.max() == 100
    assert dict(zip(nodes, res_ll.max())) == {
        "1": 100,
        "2": 100,
        "3": 10,
        "4": 20,
        "5": 5,
    }

    assert res.count() == 8
    assert res_v.count() == 5
    assert dict(zip(nodes, res_ll.count())) == {"1": 5, "2": 3, "3": 4, "4": 3, "5": 1}

    assert res.mean() == res.average() == 18.5
    assert res_v.mean() == res_v.average() == 26.2
    assert res_ll.mean() == res_ll.average()
    assert dict(zip(nodes, res_ll.mean())) == {
        "1": 26.2,
        "2": 35.666666666666664,
        "3": 4.5,
        "4": 11.666666666666666,
        "5": 5.0,
    }


def test_pytemporalprops():
    g = Graph()
    edges_str = [
        ("1", "2", 10, 1),
        ("1", "2", 10, 2),
        ("1", "2", 100, 3),
        ("1", "4", 20, 2),
        ("2", "3", 5, 3),
        ("3", "2", 2, 4),
        ("3", "1", 1, 5),
        ("4", "3", 10, 6),
        ("4", "1", 5, 7),
        ("1", "5", 2, 8),
        ("1", "5", 1, 9),
        ("1", "5", 5, 10),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})
    res = g.edge("1", "2").properties.temporal.get("value_dec")

    assert res.sum() == 120
    assert res.min() == (1, 10)
    assert res.max() == (3, 100)
    assert res.count() == 3
    assert res.mean() == res.average() == 40.0
    assert res.median() == (2, 10)
