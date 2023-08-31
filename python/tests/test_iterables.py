import math
import sys

import pandas as pd
import pandas.core.frame
import pytest
from raphtory import Graph, GraphWithDeletions, PyDirection
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
        ("1", "5", 2, 8)
    ]
    for (src, dst, val, time) in edges_str:
        g.add_edge(time, src, dst, {'value_dec': val})

    v = g.vertex("1")
    res = sorted(v.out_edges().properties.temporal.get("value_dec").values().sum())
    assert res == [2, 20, 20]

    res = sorted(v.out_edges().properties.temporal.get("value_dec").values().count())
    assert res == [1, 1, 2]

    res = v.out_edges().properties.temporal.get("value_dec").values().sum().sum()
    assert res == 42

    res = v.out_edges().properties.temporal.get("value_dec").values().count().sum()
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
        ("1", "5", 5, 10)
    ]
    for (src, dst, val, time) in edges_str:
        g.add_edge(time, src, dst, {'value_dec': val})
    v = g.vertex("1")
    res = v.out_edges().properties.temporal.get("value_dec").values() # PyPropHistValueList([[10, 10, 10], [20], [2]])
    assert res.sum() == [120, 20, 8]
    assert res.min() == [10, 20, 1]
    assert res.max() == [100, 20, 5]
    # TODO BUG: res.len() does not equal [3, 1, 3] natural order
    assert sorted(res.len()) == [1, 3, 3]
    assert sorted(res.count()) == [1, 3, 3]
    assert res.median() == [10, 20, 2]
    assert list(res.mean()) == [40, 20, 8/3]
    assert list(res.average()) == [40, 20, 8/3]


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
        ("1", "5", 2, 8)
    ]
    for (src, dst, val, time) in edges_str:
        g.add_edge(time, src, dst, {'value_dec': val})

    v = g.vertex("1")
    result = v.out_edges().properties.temporal.get("value_dec").values().flatten()
    assert sorted(result) == [2, 10, 10, 10, 20]
    assert result.sum() == 52
    assert result.median() == 10
    assert result.mean() == 10.4
    assert result.average() == 10.4
    assert result.min() == 2
    assert result.max() == 20
    assert result.len() == 5

    assert v.out_edges().properties.get("value_dec").sum() == 32
    assert v.out_edges().properties.get("value_dec").median() == 10

    total = g.vertices.in_edges().properties.get("value_dec").sum()
    assert sorted(total) == [2, 6, 12, 15, 20]

    total = g.vertices.edges().properties.get("value_dec").sum()
    assert sorted(total) == [2, 17, 18, 35, 38]

    total = dict(zip(g.vertices().id(), g.vertices.out_edges().properties.get("value_dec").sum()))
    assert total == {1: 32, 2: 5, 3: 3, 4: 15, 5: None}

    total = g.vertices.out_edges().properties.get("value_dec").sum().sum()
    assert total == 55

    total = g.vertices.out_edges().properties.get("value_dec").sum().median()
    assert total == 5

    total = g.vertices.out_edges().properties.get("value_dec").sum().drop_none()
    assert sorted(total) == [3, 5, 15, 32]

    total = g.vertices.out_edges().properties.get("value_dec").median()
    assert list(total) == [10, 5, 10, 2, None]

    total = g.vertex("1").in_edges().properties.get("value_dec").sum()
    assert total == 6

    total = g.vertex("1").in_edges().properties.get("value_dec").median()
    assert total == 5


def test_pypropvaluelist():
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
        ("1", "5", 5, 10)
    ]
    for (src, dst, val, time) in edges_str:
        g.add_edge(time, src, dst, {'value_dec': val})
    v = g.vertex("1")
    res = g.edges().properties.get("value_dec") # PyPropValueList([100, 20, 5, 5, 5, 10, 1, 2])
    res_v = v.edges().properties.get("value_dec") # PyPropValueList([100, 5, 20, 1, 5])

    assert res.sum() == 148
    assert res_v.sum() == 131

    assert res.median() == 5
    assert res_v.median() == 5

    assert res.min() == 1
    assert res_v.min() == 1

    assert res.max() == 100
    assert res_v.max() == 100

    assert res.count() == 8
    assert res_v.count() == 5

    assert res.len() == 8
    assert res_v.count() == 5

