from __future__ import unicode_literals
from decimal import Decimal
import math
import sys
import random
import re

import pandas as pd
import pandas.core.frame
import pytest
import pyarrow as pa
from raphtory import Graph, PersistentGraph
from raphtory import algorithms
from raphtory import graph_loader
import tempfile
from math import isclose
from datetime import datetime, timezone
import string
from pathlib import Path
from pytest import fixture
from numpy.testing import assert_equal as check_arr
import os
import shutil
import numpy as np
import pickle
from utils import with_disk_graph

base_dir = Path(__file__).parent
edges = [(1, 1, 2), (2, 1, 3), (-1, 2, 1), (0, 1, 1), (7, 3, 2), (1, 1, 1)]
utc = timezone.utc


def create_graph():
    g = Graph()

    g.add_node(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_node(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_node(6, 3, {"type": "wallet", "cost": 76.0})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    return g


def create_graph_with_deletions():
    g = PersistentGraph()

    g.add_node(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_node(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_node(6, 3, {"type": "wallet", "cost": 76.0})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})
    g.edge(edges[0][1], edges[0][2]).add_metadata({"static": "test"})
    g.delete_edge(10, edges[0][1], edges[0][2])
    return g


def test_graph_len_edge_len():
    g = create_graph()

    @with_disk_graph
    def assert_len_edge_len(g):
        assert g.count_nodes() == 3
        assert g.count_edges() == 5

    assert_len_edge_len(g)


def test_graph_pickle():
    g = create_graph()
    # pickle graph
    with tempfile.TemporaryDirectory() as tmpdirname:
        pickle.dump(g, open(tmpdirname + "/graph.p", "wb"))
        # unpickle graph
        g2 = pickle.load(open(tmpdirname + "/graph.p", "rb"))

        assert g2.count_nodes() == 3
        assert g2.count_edges() == 5


def test_persistent_graph_pickle():
    g = create_graph_with_deletions()
    # pickle graph
    with tempfile.TemporaryDirectory() as tmpdirname:
        pickle.dump(g, open(tmpdirname + "/graph.p", "wb"))
        # unpickle graph
        g2 = pickle.load(open(tmpdirname + "/graph.p", "rb"))

        assert g2.count_nodes() == 3
        assert g2.count_edges() == 5


def test_id_iterable():
    g = create_graph()

    @with_disk_graph
    def check_id_iterable(g):
        assert g.nodes.id.max() == 3
        assert g.nodes.id.min() == 1
        assert set(g.nodes.id.collect()) == {1, 2, 3}
        out_neighbours = g.nodes.out_neighbours.id.collect()
        out_neighbours = (set(n) for n in out_neighbours)
        out_neighbours = dict(zip(g.nodes.id, out_neighbours))

        assert out_neighbours == {1: {1, 2, 3}, 2: {1}, 3: {2}}

    check_id_iterable(g)


def test_degree_iterable():
    g = create_graph()

    @with_disk_graph
    def check_degree_iterable(g):
        assert g.nodes.degree().min() == 2
        assert g.nodes.degree().max() == 3
        assert g.nodes.in_degree().min() == 1
        assert g.nodes.in_degree().max() == 2
        assert g.nodes.out_degree().min() == 1
        assert g.nodes.out_degree().max() == 3
        assert isclose(g.nodes.degree().mean(), 7 / 3)
        assert g.nodes.degree().sum() == 7
        degrees = g.nodes.degree().collect()
        degrees.sort()
        assert degrees == [2, 2, 3]

    check_degree_iterable(g)


def test_nodes_time_iterable():
    g = create_graph()

    @with_disk_graph
    def check(g):
        assert g.nodes.earliest_time.min() == -1
        assert g.nodes.latest_time.max() == 7

    check(g)


def test_graph_has_edge():
    g = create_graph()

    @with_disk_graph
    def check(g):
        assert not g.window(-1, 1).has_edge(1, 3)
        assert g.window(-1, 3).has_edge(1, 3)
        assert not g.window(10, 11).has_edge(1, 3)

    check(g)


def test_graph_has_node():
    g = create_graph()

    @with_disk_graph
    def check(g):
        assert g.has_node(3)

    check(g)


def test_windowed_graph_has_node():
    g = create_graph()

    @with_disk_graph
    def check(g):
        assert g.window(-1, 1).has_node(1)

    check(g)


def test_windowed_graph_get_node():
    g = create_graph()

    @with_disk_graph
    def check(g):
        view = g.window(0, sys.maxsize)

        assert view.node(1).id == 1
        assert view.node(10) is None
        assert view.node(1).degree() == 3

    check(g)


def test_edge_sorting():
    g = create_graph()

    @with_disk_graph
    def check(g):
        assert sorted(g.edges, key=lambda e: e.id) == sorted(g.edges)
        assert sorted(g.edges.explode(), key=lambda e: (*e.id, e.time)) == sorted(
            g.edges.explode()
        )

    check(g)


def test_windowed_graph_degree():
    g = create_graph()

    @with_disk_graph
    def check(g):
        view = g.window(0, sys.maxsize)

        degrees = [v.degree() for v in view.nodes]
        degrees.sort()

        assert degrees == [2, 2, 3]

        in_degrees = [v.in_degree() for v in view.nodes]
        in_degrees.sort()

        assert in_degrees == [1, 1, 2]

        out_degrees = [v.out_degree() for v in view.nodes]
        out_degrees.sort()

        assert out_degrees == [0, 1, 3]

    check(g)


def test_windowed_graph_get_edge():
    g = create_graph()

    @with_disk_graph
    def check(g):
        max_size = sys.maxsize
        min_size = -sys.maxsize - 1

        view = g.window(min_size, max_size)

        assert (view.edge(1, 3).src.id, view.edge(1, 3).dst.id) == (1, 3)
        assert view.edge(2, 3) is None
        assert view.edge(6, 5) is None

        assert (view.node(1).id, view.node(3).id) == (1, 3)

        view = g.window(2, 3)
        assert (view.edge(1, 3).src.id, view.edge(1, 3).dst.id) == (1, 3)

        view = g.window(3, 7)
        assert view.edge(1, 3) is None

    check(g)


def test_windowed_graph_edges():
    g = create_graph()

    @with_disk_graph
    def check(g):
        view = g.window(0, sys.maxsize)

        tedges = [v.edges for v in view.nodes]
        edges = []
        for e_iter in tedges:
            for e in e_iter:
                edges.append([e.src.id, e.dst.id])

        assert edges == [[1, 1], [1, 2], [1, 3], [1, 2], [3, 2], [1, 3], [3, 2]]

        tedges = [v.in_edges for v in view.nodes]
        in_edges = []
        for e_iter in tedges:
            for e in e_iter:
                in_edges.append([e.src.id, e.dst.id])

        assert in_edges == [[1, 1], [1, 2], [3, 2], [1, 3]]

        tedges = [v.out_edges for v in view.nodes]
        out_edges = []
        for e_iter in tedges:
            for e in e_iter:
                out_edges.append([e.src.id, e.dst.id])

        assert out_edges == [[1, 1], [1, 2], [1, 3], [3, 2]]

    check(g)


def test_windowed_graph_node_ids():
    g = create_graph()

    @with_disk_graph
    def check(g):
        vs = [v for v in g.window(-1, 2).nodes.id]
        vs.sort()
        assert vs == [1, 2]  # this makes clear that the end of the range is exclusive

        vs = [v for v in g.window(-5, 3).nodes.id]
        vs.sort()
        assert vs == [1, 2, 3]

    check(g)


def test_windowed_graph_nodes():
    g = create_graph()

    @with_disk_graph
    def check(g):
        view = g.window(-1, 0)
        nodes = list(view.nodes.id)
        assert nodes == [1, 2]

    check(g)


def test_windowed_graph_neighbours():
    g = create_graph()

    @with_disk_graph
    def check(g):
        max_size = sys.maxsize
        min_size = -sys.maxsize - 1

        view = g.window(min_size, max_size)

        neighbours = view.nodes.neighbours.id.collect()
        assert neighbours == [[1, 2, 3], [1, 3], [1, 2]]

        in_neighbours = view.nodes.in_neighbours.id.collect()
        assert in_neighbours == [[1, 2], [1, 3], [1]]

        out_neighbours = view.nodes.out_neighbours.id.collect()
        assert out_neighbours == [[1, 2, 3], [1], [2]]

    check(g)


def test_name():
    g = Graph()
    g.add_node(1, "Ben")
    g.add_edge(1, "Ben", "Hamza")
    assert g.node("Ben").name == "Ben"
    assert g.node("Hamza").name == "Hamza"


def test_getitem():
    g = Graph()
    g.add_node(0, 1, {"cost": 0})
    g.add_node(1, 1, {"cost": 1})

    @with_disk_graph
    def check(g):
        assert (
            g.node(1).properties.temporal.get("cost")
            == g.node(1).properties.temporal["cost"]
        )

    check(g)


def test_entity_history_date_time():
    g = Graph()
    g.add_node(0, 1)
    g.add_node(1, 1)
    g.add_node(2, 1)
    v = g.add_node(3, 1)
    g.add_edge(0, 1, 2)
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    e = g.add_edge(3, 1, 2)

    full_history_1 = [
        datetime(1970, 1, 1, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 2000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 3000, tzinfo=utc),
    ]

    full_history_2 = [
        datetime(1970, 1, 1, 0, 0, 0, 4000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 5000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 6000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc),
    ]

    windowed_history = [
        datetime(1970, 1, 1, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=utc),
    ]

    assert v.history_date_time() == full_history_1
    assert v.window(0, 2).history_date_time() == windowed_history
    assert e.history_date_time() == full_history_1
    assert e.window(0, 2).history_date_time() == windowed_history

    g.add_edge(4, 1, 3)
    g.add_edge(5, 1, 3)
    g.add_edge(6, 1, 3)
    g.add_edge(7, 1, 3)

    assert g.edges.history_date_time() == [full_history_1, full_history_2]
    assert g.nodes.in_edges.history_date_time() == [
        [],
        [full_history_1],
        [full_history_2],
    ]

    assert g.nodes.earliest_date_time == [
        datetime(1970, 1, 1, tzinfo=utc),
        datetime(1970, 1, 1, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 4000, tzinfo=utc),
    ]
    assert g.nodes.latest_date_time == [
        datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 3000, tzinfo=utc),
        datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc),
    ]

    assert g.nodes.neighbours.latest_date_time.collect() == [
        [
            datetime(1970, 1, 1, 0, 0, 0, 3000, tzinfo=utc),
            datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc),
        ],
        [datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc)],
        [datetime(1970, 1, 1, 0, 0, 0, 7000, tzinfo=utc)],
    ]

    assert g.nodes.neighbours.earliest_date_time.collect() == [
        [
            datetime(1970, 1, 1, tzinfo=utc),
            datetime(1970, 1, 1, 0, 0, 0, 4000, tzinfo=utc),
        ],
        [datetime(1970, 1, 1, tzinfo=utc)],
        [datetime(1970, 1, 1, tzinfo=utc)],
    ]


def test_graph_properties():
    g = create_graph()

    props = {
        "prop 1": 1,
        "prop 2": "hi",
        "prop 3": True,
        "prop 4": [1, 2],
        "prop 5": {"x": 1, "y": "ok"},
    }
    g.add_metadata(props)

    sp = g.metadata.keys()
    sp.sort()
    assert sp == ["prop 1", "prop 2", "prop 3", "prop 4", "prop 5"]
    assert g.metadata["prop 1"] == 1
    assert g.metadata["prop 4"] == [1, 2]
    assert g.metadata["prop 5"] == {"x": 1, "y": "ok"}

    props = {"prop 4": 11, "prop 5": "world", "prop 6": False}
    g.add_properties(1, props)

    props = {"prop 6": True}
    g.add_properties(2, props)

    def history_test(key, value):
        if value is None:
            assert g.properties.temporal.get(key) is None
        else:
            assert g.properties.temporal.get(key).items() == value

    history_test("prop 1", None)
    history_test("prop 2", None)
    history_test("prop 3", None)
    history_test("prop 4", [(1, 11)])
    history_test("prop 5", [(1, "world")])
    history_test("prop 6", [(1, False), (2, True)])
    history_test("undefined", None)

    def time_history_test(time, key, value):
        if value is None:
            assert g.before(time + 1).properties.temporal.get(key) is None
        else:
            assert g.before(time + 1).properties.temporal.get(key).items() == value

    time_history_test(2, "prop 6", [(1, False), (2, True)])
    time_history_test(1, "static prop", None)

    def time_static_property_test(time, key, value):
        assert g.at(time).metadata.get(key) == value

    def static_property_test(key, value):
        assert g.metadata.get(key) == value

    time_static_property_test(1, "prop 1", 1)
    time_static_property_test(100, "prop 1", 1)
    static_property_test("prop 1", 1)
    static_property_test("prop 3", True)

    # testing property
    def time_property_test(time, key, value):
        assert g.at(time).properties.get(key) == value

    def property_test(key, value):
        assert g.properties.get(key) == value

    static_property_test("prop 2", "hi")
    property_test("prop 2", None)
    time_static_property_test(2, "prop 3", True)
    time_property_test(2, "prop 3", None)

    # testing properties
    assert g.properties.as_dict() == {"prop 4": 11, "prop 5": "world", "prop 6": True}

    assert g.properties.temporal.latest() == {
        "prop 4": 11,
        "prop 5": "world",
        "prop 6": True,
    }
    assert g.before(3).properties.as_dict() == {
        "prop 5": "world",
        "prop 6": True,
        "prop 4": 11,
    }

    # testing property histories
    assert g.properties.temporal.histories() == {
        "prop 4": [(1, 11)],
        "prop 5": [(1, "world")],
        "prop 6": [(1, False), (2, True)],
    }

    assert g.before(3).properties.temporal.histories() == {
        "prop 4": [(1, 11)],
        "prop 5": [(1, "world")],
        "prop 6": [(1, False), (2, True)],
    }

    # testing property names
    expected_names = sorted(["prop 4", "prop 5", "prop 6"])
    assert sorted(g.properties.keys()) == expected_names

    expected_names_no_static = sorted(["prop 4", "prop 5", "prop 6"])
    assert sorted(g.properties.temporal.keys()) == expected_names_no_static

    assert sorted(g.before(2).properties.temporal.keys()) == expected_names_no_static

    # testing has_property
    assert "prop 4" in g.properties
    assert "prop 7" not in g.properties
    assert "prop 7" not in g.before(2).properties
    assert "prop 1" in g.metadata
    assert "prop 2" in g.before(2).metadata
    assert "static prop" not in g.metadata


def create_graph_history_1():
    g = Graph()
    g.add_edge(1, 1, 1)
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    v = g.add_node(1, 1, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    v.add_updates(2, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    v.add_updates(3, props_t3)
    v.add_metadata({"static prop": 123})
    return g


def test_node_properties():
    g = create_graph_history_1()

    @with_disk_graph
    def check(g):
        # testing property history
        def history_test(key, value):
            if value is None:
                assert g.node(1).properties.temporal.get(key) is None
                assert g.nodes.properties.temporal.get(key) is None
                assert g.nodes.out_neighbours.properties.temporal.get(key) is None
            else:
                assert g.node(1).properties.temporal.get(key).items() == value
                assert g.nodes.properties.temporal.get(key).items() == [value]
                assert g.nodes.out_neighbours.properties.temporal.get(key).items() == [
                    [value]
                ]

        history_test("prop 1", [(1, 1), (2, 2)])
        history_test("prop 2", [(2, 0.6), (3, 0.9)])
        history_test("prop 3", [(1, "hi"), (3, "hello")])
        history_test("prop 4", [(1, True), (2, False), (3, True)])
        history_test("undefined", None)

        def time_history_test(time, key, value):
            if value is None:
                assert g.at(time).node(1).properties.temporal.get(key) is None
                assert g.at(time).nodes.properties.temporal.get(key) is None
                assert (
                    g.at(time).nodes.out_neighbours.properties.temporal.get(key) is None
                )
            else:
                assert g.at(time).node(1).properties.temporal.get(key).items() == value
                assert g.at(time).nodes.properties.temporal.get(key).items() == [value]
                assert g.at(time).nodes.out_neighbours.properties.temporal.get(
                    key
                ).items() == [[value]]

        time_history_test(1, "prop 4", [(1, True)])
        time_history_test(1, "static prop", None)

        def time_static_property_test(time, key, value):
            gg = g.before(time + 1)
            if value is None:
                assert gg.node(1).metadata.get(key) is None
                assert gg.nodes.metadata.get(key) is None
                assert gg.nodes.out_neighbours.metadata.get(key) is None
            else:
                assert gg.node(1).metadata.get(key) == value
                assert gg.nodes.metadata.get(key) == [value]
                assert gg.nodes.out_neighbours.metadata.get(key) == [[value]]

        def static_property_test(key, value):
            if value is None:
                assert g.node(1).metadata.get(key) is None
                assert g.nodes.metadata.get(key) is None
                assert g.nodes.out_neighbours.metadata.get(key) is None
            else:
                assert g.node(1).metadata.get(key) == value
                assert g.nodes.metadata.get(key) == [value]
                assert g.nodes.out_neighbours.metadata.get(key) == [[value]]

        time_static_property_test(1, "static prop", 123)
        time_static_property_test(100, "static prop", 123)
        static_property_test("static prop", 123)
        static_property_test("prop 4", None)

        # testing property
        def time_property_test(time, key, value):
            gg = g.before(time + 1)
            if value is None:
                assert gg.node(1).properties.get(key) is None
            else:
                assert gg.node(1).properties.get(key) == value
                assert gg.nodes.properties.get(key) == [value]
                assert gg.nodes.out_neighbours.properties.get(key) == [[value]]

        def meta_test(key, value):
            if value is None:
                assert g.node(1).metadata.get(key) is None
                assert g.nodes.metadata.get(key) is None
                assert g.nodes.out_neighbours.metadata.get(key) is None
            else:
                assert g.node(1).metadata.get(key) == value
                assert g.nodes.metadata.get(key) == [value]
                assert g.nodes.out_neighbours.metadata.get(key) == [[value]]

        def no_static_property_test(key, value):
            if value is None:
                assert g.node(1).properties.temporal.get(key) is None
                assert g.nodes.properties.temporal.get(key) is None
                assert g.nodes.out_neighbours.properties.temporal.get(key) is None
            else:
                assert g.node(1).properties.temporal.get(key).value() == value
                assert g.nodes.properties.temporal.get(key).value() == [value]
                assert g.nodes.out_neighbours.properties.temporal.get(key).value() == [
                    [value]
                ]

        meta_test("static prop", 123)
        assert g.node(1).metadata["static prop"] == 123
        no_static_property_test("static prop", None)
        no_static_property_test("prop 1", 2)
        time_property_test(2, "prop 2", 0.6)
        time_property_test(1, "prop 2", None)

        # testing properties
        assert g.node(1).properties == {
            "prop 2": 0.9,
            "prop 3": "hello",
            "prop 1": 2,
            "prop 4": True,
        }

        assert g.node(1).metadata == {
            "static prop": 123,
        }

        # find all nodes that match properties
        [n] = g.find_nodes(
            {
                "prop 3": "hello",
                "prop 1": 2,
            }
        )
        assert n == g.node(1)

        empty_list = g.find_nodes({"prop 1": 2, "prop 3": "hi"})
        assert len(empty_list) == 0

        assert g.nodes.properties == {
            "prop 2": [0.9],
            "prop 3": ["hello"],
            "prop 1": [2],
            "prop 4": [True],
        }

        assert g.nodes.out_neighbours.properties == {
            "prop 2": [[0.9]],
            "prop 3": [["hello"]],
            "prop 1": [[2]],
            "prop 4": [[True]],
        }

        assert g.nodes.metadata == {
            "static prop": [123],
        }

        assert g.nodes.out_neighbours.metadata == {
            "static prop": [[123]],
        }

        assert g.node(1).properties.temporal.latest() == {
            "prop 2": 0.9,
            "prop 3": "hello",
            "prop 1": 2,
            "prop 4": True,
        }
        assert g.nodes.properties.temporal.latest() == {
            "prop 2": [0.9],
            "prop 3": ["hello"],
            "prop 1": [2],
            "prop 4": [True],
        }
        assert g.nodes.out_neighbours.properties.temporal.latest() == {
            "prop 2": [[0.9]],
            "prop 3": [["hello"]],
            "prop 1": [[2]],
            "prop 4": [[True]],
        }

        assert g.before(3).node(1).properties == {
            "prop 1": 2,
            "prop 4": False,
            "prop 2": 0.6,
            "prop 3": "hi",
        }
        assert g.before(3).nodes.properties == {
            "prop 1": [2],
            "prop 4": [False],
            "prop 2": [0.6],
            "prop 3": ["hi"],
        }
        assert g.before(3).nodes.out_neighbours.properties == {
            "prop 1": [[2]],
            "prop 4": [[False]],
            "prop 2": [[0.6]],
            "prop 3": [["hi"]],
        }

        assert g.before(3).node(1).metadata == {
            "static prop": 123,
        }
        assert g.before(3).nodes.metadata == {
            "static prop": [123],
        }
        assert g.before(3).nodes.out_neighbours.metadata == {
            "static prop": [[123]],
        }

        # testing property histories
        assert g.node(1).properties.temporal == {
            "prop 3": [(1, "hi"), (3, "hello")],
            "prop 1": [(1, 1), (2, 2)],
            "prop 4": [(1, True), (2, False), (3, True)],
            "prop 2": [(2, 0.6), (3, 0.9)],
        }
        assert g.nodes.properties.temporal == {
            "prop 3": [[(1, "hi"), (3, "hello")]],
            "prop 1": [[(1, 1), (2, 2)]],
            "prop 4": [[(1, True), (2, False), (3, True)]],
            "prop 2": [[(2, 0.6), (3, 0.9)]],
        }
        assert g.nodes.out_neighbours.properties.temporal == {
            "prop 3": [[[(1, "hi"), (3, "hello")]]],
            "prop 1": [[[(1, 1), (2, 2)]]],
            "prop 4": [[[(1, True), (2, False), (3, True)]]],
            "prop 2": [[[(2, 0.6), (3, 0.9)]]],
        }

        assert g.at(2).node(1).properties.temporal == {
            "prop 2": [(2, 0.6)],
            "prop 4": [(2, False)],
            "prop 1": [(2, 2)],
        }
        assert g.before(3).nodes.properties.temporal == {
            "prop 2": [[(2, 0.6)]],
            "prop 4": [[(1, True), (2, False)]],
            "prop 1": [[(1, 1), (2, 2)]],
            "prop 3": [[(1, "hi")]],
        }
        assert g.before(3).nodes.out_neighbours.properties.temporal == {
            "prop 2": [[[(2, 0.6)]]],
            "prop 4": [[[(1, True), (2, False)]]],
            "prop 1": [[[(1, 1), (2, 2)]]],
            "prop 3": [[[(1, "hi")]]],
        }

        # testing property names
        expected_names = sorted(["prop 4", "prop 1", "prop 2", "prop 3"])
        assert sorted(g.node(1).properties.keys()) == expected_names
        assert sorted(g.nodes.properties.keys()) == expected_names
        assert sorted(g.nodes.out_neighbours.properties.keys()) == expected_names

        expected_names_no_static = sorted(["prop 4", "prop 1", "prop 2", "prop 3"])
        assert sorted(g.node(1).properties.temporal.keys()) == expected_names_no_static
        assert sorted(g.nodes.properties.temporal.keys()) == expected_names_no_static
        assert (
            sorted(g.nodes.out_neighbours.properties.temporal.keys())
            == expected_names_no_static
        )

        expected_names_no_static_at_1 = ["prop 1", "prop 2", "prop 3", "prop 4"]
        assert sorted(g.at(1).node(1).properties.temporal.keys()) == [
            "prop 1",
            "prop 3",
            "prop 4",
        ]
        assert (
            sorted(g.at(1).nodes.properties.temporal.keys())
            == expected_names_no_static_at_1
        )
        assert (
            sorted(g.at(1).nodes.out_neighbours.properties.temporal.keys())
            == expected_names_no_static_at_1
        )

        # testing has_property
        assert "prop 4" in g.node(1).properties
        assert "prop 4" in g.nodes.properties
        assert "prop 4" in g.nodes.out_neighbours.properties

        assert "prop 2" in g.node(1).properties
        assert "prop 2" in g.nodes.properties
        assert "prop 2" in g.nodes.out_neighbours.properties

        assert "prop 5" not in g.node(1).properties
        assert "prop 5" not in g.nodes.properties
        assert "prop 5" not in g.nodes.out_neighbours.properties

        assert (
            "prop 2" not in g.at(1).node(1).properties.as_dict()
        )  # TODO should be as dict?
        #       assert "prop 2" not in g.at(1).nodes.properties #TODO Do these make sense any more?
        #        assert "prop 2" not in g.at(1).nodes.out_neighbours.properties #TODO Do these make sense any more?

        assert "static prop" in g.node(1).metadata
        assert "static prop" in g.nodes.metadata
        assert "static prop" in g.nodes.out_neighbours.metadata

        assert "static prop" in g.at(1).node(1).metadata
        assert "static prop" in g.at(1).nodes.metadata
        assert "static prop" in g.at(1).nodes.out_neighbours.metadata

        assert "static prop" not in g.at(1).node(1).properties.temporal
        assert "static prop" not in g.at(1).nodes.properties.temporal
        assert "static prop" not in g.at(1).nodes.out_neighbours.properties.temporal

        assert "static prop" in g.node(1).metadata
        assert "static prop" in g.nodes.metadata
        assert "static prop" in g.nodes.out_neighbours.metadata

        assert "prop 2" not in g.node(1).metadata
        assert "prop 2" not in g.nodes.metadata
        assert "prop 2" not in g.nodes.out_neighbours.metadata

        assert "static prop" in g.at(1).node(1).metadata
        assert "static prop" in g.at(1).nodes.metadata
        assert "static prop" in g.at(1).nodes.out_neighbours.metadata

    check(g)


def test_decimal_property():
    g = Graph()
    decimal_properties = {
        "d_max": Decimal("9999999999999999999999999999.999999999"),
        "d_min": Decimal("-9999999999999999999999999999.999999999"),
        "d_int": Decimal("9999999999999999999999999999"),
        "d_small_neg": Decimal("-0.1"),
        "d_small_pos": Decimal("0.1"),
        "d_medium": Decimal("104447267751554560119.000000000"),
    }
    g.add_edge(0, 1, 1, decimal_properties)
    g.add_node(2, 2, decimal_properties)
    n = g.node(2)
    n.add_updates(3, decimal_properties)
    e = g.edge(1, 1)
    e.add_updates(3, decimal_properties)

    with pytest.raises(Exception):
        g.add_edge(
            0,
            1,
            1,
            {
                "d_extra_max_super_ultra_x_x": Decimal(
                    "9999999999999999999999999999999999999999999999999999.999999999"
                )
            },
        )

    @with_disk_graph
    def check(g):
        assert g.node(2).properties.temporal.get("d_max").items() == [
            (2, Decimal("9999999999999999999999999999.999999999")),
            (3, Decimal("9999999999999999999999999999.999999999")),
        ]

        assert g.edge(1, 1).properties.temporal.get("d_min").items() == [
            (0, Decimal("-9999999999999999999999999999.999999999")),
            (3, Decimal("-9999999999999999999999999999.999999999")),
        ]

        assert g.node(2).properties.temporal == {
            "d_max": [
                (2, Decimal("9999999999999999999999999999.999999999")),
                (3, Decimal("9999999999999999999999999999.999999999")),
            ],
            "d_min": [
                (2, Decimal("-9999999999999999999999999999.999999999")),
                (3, Decimal("-9999999999999999999999999999.999999999")),
            ],
            "d_int": [
                (2, Decimal("9999999999999999999999999999")),
                (3, Decimal("9999999999999999999999999999")),
            ],
            "d_small_neg": [(2, Decimal("-0.1")), (3, Decimal("-0.1"))],
            "d_small_pos": [(2, Decimal("0.1")), (3, Decimal("0.1"))],
            "d_medium": [
                (2, Decimal("104447267751554560119.000000000")),
                (3, Decimal("104447267751554560119.000000000")),
            ],
        }

        assert g.node(2).properties.temporal.get("d_small_pos").sum() == Decimal("0.2")
        assert g.node(2).properties.temporal.get("d_small_neg").sum() == Decimal("-0.2")
        assert g.node(2).properties.temporal.get("d_medium").sum() == Decimal(
            "208894535503109120238.000000000"
        )

    check(g)


def test_edge_properties():
    g = create_graph_edge_properties()

    @with_disk_graph
    def check_temporal_properties(g):
        # testing property history
        assert g.edge(1, 2).properties.temporal.get("prop 1") == [(1, 1), (2, 2)]
        assert g.edge(1, 2).properties.temporal.get("prop 2") == [(2, 0.6), (3, 0.9)]
        assert g.edge(1, 2).properties.temporal.get("prop 3") == [
            (1, "hi"),
            (3, "hello"),
        ]
        assert g.edge(1, 2).properties.temporal.get("prop 4") == [
            (1, True),
            (2, False),
            (3, True),
        ]
        assert g.edge(1, 2).properties.temporal.get("undefined") is None
        assert g.at(1).edge(1, 2).properties.temporal.get("prop 4") == [(1, True)]
        assert g.at(1).edge(1, 2).properties.temporal.get("static prop") is None

        assert g.at(1).edge(1, 2).properties.temporal == {
            "prop 4": [(1, True)],
            "prop 1": [(1, 1)],
            "prop 3": [(1, "hi")],
        }

        assert g.edge(1, 2).properties.temporal.latest() == {
            "prop 2": 0.9,
            "prop 3": "hello",
            "prop 1": 2,
            "prop 4": True,
        }

        # testing property histories
        assert g.edge(1, 2).properties.temporal == {
            "prop 3": [(1, "hi"), (3, "hello")],
            "prop 1": [(1, 1), (2, 2)],
            "prop 4": [(1, True), (2, False), (3, True)],
            "prop 2": [(2, 0.6), (3, 0.9)],
        }

        assert g.at(2).edge(1, 2).properties.temporal == {
            "prop 2": [(2, 0.6)],
            "prop 4": [(2, False)],
            "prop 1": [(2, 2)],
        }

        assert g.after(2).edge(1, 2).properties.temporal == {
            "prop 2": [(3, 0.9)],
            "prop 3": [(3, "hello")],
            "prop 4": [(3, True)],
        }

        assert sorted(g.edge(1, 2).properties.temporal.keys()) == sorted(
            ["prop 4", "prop 1", "prop 2", "prop 3"]
        )

        assert sorted(g.at(1).edge(1, 2).properties.temporal.keys()) == [
            "prop 1",
            "prop 3",
            "prop 4",
        ]

        # find all edges that match properties
        [e] = g.at(1).find_edges({"prop 1": 1, "prop 3": "hi"})
        assert e == g.edge(1, 2)

        empty_list = g.at(1).find_edges({"prop 1": 1, "prop 3": "hx"})
        assert len(empty_list) == 0

        # testing has_property
        assert "prop 4" in g.edge(1, 2).properties
        assert "prop 2" in g.edge(1, 2).properties
        assert "prop 5" not in g.edge(1, 2).properties
        assert (
            "prop 2" not in g.at(1).edge(1, 2).properties.as_dict()
        )  # TODO should be as dict? - Properties now returns all keys here

    def check(g):
        assert g.at(1).edge(1, 2).metadata.get("static prop") == 123
        assert g.before(101).edge(1, 2).metadata.get("static prop") == 123
        assert g.edge(1, 2).metadata.get("static prop") == 123
        assert g.edge(1, 2).metadata.get("prop 4") is None

        # testing property
        assert g.edge(1, 2).metadata.get("static prop") == 123
        assert g.edge(1, 2).metadata["static prop"] == 123
        assert g.edge(1, 2).properties.temporal.get("static prop") is None
        assert g.edge(1, 2).properties.temporal.get("prop 1").value() == 2
        assert g.at(2).edge(1, 2).properties.get("prop 2") == 0.6
        assert g.at(1).edge(1, 2).properties.get("prop 2") is None

        # testing properties
        assert g.edge(1, 2).properties == {
            "prop 2": 0.9,
            "prop 3": "hello",
            "prop 1": 2,
            "prop 4": True,
        }

        assert g.before(3).edge(1, 2).properties == {
            "prop 1": 2,
            "prop 4": False,
            "prop 2": 0.6,
            "prop 3": "hi",
        }

        assert g.edge(1, 2).metadata == {
            "static prop": 123,
        }

        assert g.before(3).edge(1, 2).metadata == {
            "static prop": 123,
        }

        # testing property names
        assert sorted(g.edge(1, 2).properties.keys()) == sorted(
            ["prop 4", "prop 1", "prop 2", "prop 3"]
        )

        assert "static prop" in g.edge(1, 2).metadata
        assert "static prop" in g.at(1).edge(1, 2).metadata
        assert "static prop" not in g.at(1).edge(1, 2).properties.temporal

        assert "static prop" in g.edge(1, 2).metadata
        assert "prop 2" not in g.edge(1, 2).metadata
        assert "static prop" in g.at(1).edge(1, 2).metadata

    check_temporal_properties(g)
    check(g)


def create_graph_edge_properties():
    g = Graph()
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    e = g.add_edge(1, 1, 2, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    e.add_updates(2, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    e.add_updates(3, props_t3)

    e.add_metadata({"static prop": 123})
    return g


def test_edge_metadata_layers():
    g = Graph()
    g.add_edge(0, 1, 2, layer="a")
    g.add_edge(0, 1, 2)
    g.edge(1, 2).add_metadata({"test": 1})
    metadata_exploded = g.layer("a").edges.explode().metadata
    assert metadata_exploded.values() == [[None]]
    assert metadata_exploded.keys() == ["test"]


def test_temporal_edge_properties_layers():
    g = Graph()
    g.add_edge(0, 1, 2, {"test": 1}, layer="a")
    g.add_edge(0, 1, 2)
    temporal_exploded = g.default_layer().edges.explode().properties.temporal
    assert temporal_exploded.keys() == ["test"]
    assert temporal_exploded.values() == [[[]]]


def test_arrow_array_properties():
    g = Graph()
    days = pa.array([1, 12, 17, 23, 28], type=pa.uint8())
    g.add_edge(1, 1, 2, {"prop1": 1, "prop2": 2, "prop3": days})
    e = g.edge(1, 2)
    assert e.properties["prop3"] == days


def test_map_and_list_property():
    g = Graph()
    g.add_edge(0, 1, 2, {"map": {"test": 1, "list": [1, 2, 3]}})
    e_props = g.edge(1, 2).properties
    assert "map" in e_props
    assert e_props["map"]["test"] == 1
    assert e_props["map"]["list"] == [1, 2, 3]


def test_exploded_edge_time():
    g = graph_loader.lotr_graph()

    @with_disk_graph
    def check(g):
        e = g.edge("Frodo", "Gandalf")
        his = e.history()
        exploded_his = []
        for ee in e.explode():
            exploded_his.append(ee.time)
        check_arr(his, exploded_his)

    check(g)


def test_algorithms():
    g = Graph()
    lotr_graph = graph_loader.lotr_graph()
    g.add_edge(1, 1, 2, {"prop1": 1})
    g.add_edge(2, 2, 3, {"prop1": 1})
    g.add_edge(3, 3, 1, {"prop1": 1})

    @with_disk_graph
    def check(g):
        view = g.window(0, 4)
        triangles = algorithms.local_triangle_count(view, 1)
        average_degree = algorithms.average_degree(view)
        max_out_degree = algorithms.max_out_degree(view)
        max_in_degree = algorithms.max_in_degree(view)
        min_out_degree = algorithms.min_out_degree(view)
        min_in_degree = algorithms.min_in_degree(view)
        graph_density = algorithms.directed_graph_density(view)
        clustering_coefficient = algorithms.local_clustering_coefficient(view, 1)

        assert triangles == 1
        assert average_degree == 2.0
        assert graph_density == 0.5
        assert max_out_degree == 1
        assert max_in_degree == 1
        assert min_out_degree == 1
        assert min_in_degree == 1
        assert clustering_coefficient == 1.0

        lotr_clustering_coefficient = algorithms.local_clustering_coefficient(
            lotr_graph, "Frodo"
        )
        lotr_local_triangle_count = algorithms.local_triangle_count(lotr_graph, "Frodo")
        assert lotr_clustering_coefficient == 0.1984313725490196
        assert lotr_local_triangle_count == 253

    check(g)


def test_graph_time_api():
    g = create_graph()

    @with_disk_graph
    def check(g):
        earliest_time = g.earliest_time
        latest_time = g.latest_time
        assert len(list(g.rolling(1))) == latest_time - earliest_time + 1
        assert len(list(g.expanding(2))) == math.ceil(
            (latest_time + 1 - earliest_time) / 2
        )

        w = g.window(2, 6)
        assert len(list(w.rolling(window=10, step=3))) == 2

    check(g)


def test_save_missing_dir():
    g = create_graph()
    tmpdirname = tempfile.TemporaryDirectory()
    inner_folder = "".join(random.choice(string.ascii_letters) for _ in range(10))
    graph_path = tmpdirname.name + "/" + inner_folder + "/test_graph.bin"
    with pytest.raises(Exception):
        g.save_to_file(graph_path)


def test_save_load_graph():
    g = create_graph()
    g.add_node(1, 11, {"type": "wallet", "balance": 99.5})
    g.add_node(2, 12, {"type": "wallet", "balance": 10.0})
    g.add_node(3, 13, {"type": "wallet", "balance": 76.0})
    g.add_edge(4, 11, 12, {"prop1": 1, "prop2": 9.8, "prop3": "test"})
    g.add_edge(5, 12, 13, {"prop1": 1321, "prop2": 9.8, "prop3": "test"})
    g.add_edge(6, 13, 11, {"prop1": 645, "prop2": 9.8, "prop3": "test"})

    @with_disk_graph
    def check(g):
        view = g.window(0, 10)
        assert g.has_node(13)
        assert view.node(13).in_degree() == 1
        assert view.node(13).out_degree() == 1
        assert view.node(13).degree() == 2

        triangles = algorithms.local_triangle_count(
            view, 13
        )  # How many triangles is 13 involved in
        assert triangles == 1

        v = view.node(11)
        assert v.properties.temporal == {
            "type": [(1, "wallet")],
            "balance": [(1, 99.5)],
        }

        with tempfile.TemporaryDirectory() as tmpdirname:
            graph_abs_path = os.path.join(tmpdirname, "test_graph.bin")
            g.save_to_file(graph_abs_path)

            del g

            g = Graph.load_from_file(graph_abs_path)

            view = g.window(0, 10)
            assert g.has_node(13)
            assert view.node(13).in_degree() == 1
            del view
            del g

    check(g)


def test_graph_at():
    g = create_graph()

    @with_disk_graph
    def check(g):
        view = g.at(1)
        assert view.node(1).degree() == 2
        assert view.node(2).degree() == 1

        view = g.before(3)
        assert view.node(1).degree() == 3
        assert view.node(3).degree() == 1

        view = g.before(8)
        assert view.node(3).degree() == 2

        view = g.after(6)
        assert view.node(2).degree() == 1
        assert view.node(3).degree() == 1

    check(g)


def test_add_node_string_multiple_types_fail():
    g = Graph()

    g.add_node(0, 1, {})
    with pytest.raises(Exception):
        g.add_node(1, "haaroon", {})

    assert g.has_node(1)


def test_add_node_string():
    g = Graph()

    g.add_node(0, "haaroon", {})

    assert g.has_node("haaroon")


def test_add_edge_string():
    g = Graph()

    g.add_edge(1, "haaroon", "ben", {})

    assert g.has_node("haaroon")
    assert g.has_node("ben")

    assert g.has_edge("haaroon", "ben")


def test_add_edge_num():
    g = Graph()

    g.add_edge(1, 1, 2, {})

    assert g.has_node(1)
    assert g.has_node(2)

    assert g.has_edge(1, 2)


def test_all_neighbours_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(4, 2, 4, {})

    @with_disk_graph
    def check(g):
        view = g.before(3)
        v = view.node(2)
        assert list(v.window(0, 2).in_neighbours.id) == [1]
        assert list(v.window(0, 2).out_neighbours.id) == [3]
        assert list(v.window(0, 2).neighbours.id) == [1, 3]

    check(g)


def test_all_degrees_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(3, 4, 2, {})
    g.add_edge(4, 2, 4, {})
    g.add_edge(5, 2, 1, {})

    @with_disk_graph
    def check(g):
        view = g.before(5)
        v = view.node(2)
        assert v.window(0, 4).in_degree() == 3
        assert v.after(1).in_degree() == 2
        assert v.before(3).in_degree() == 2
        assert v.window(0, 4).out_degree() == 1
        assert v.after(1).out_degree() == 1
        assert v.before(end=3).out_degree() == 1
        assert v.window(0, 4).degree() == 3
        assert v.after(1).degree() == 2
        assert v.before(end=3).degree() == 2

    check(g)


def test_all_edge_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(3, 4, 2, {})
    g.add_edge(4, 2, 4, {})
    g.add_edge(5, 2, 1, {})

    @with_disk_graph
    def check(g):
        view = g.before(5)
        v = view.node(2)
        assert sorted(v.window(0, 4).in_edges.src.id) == [1, 3, 4]
        assert sorted(v.before(end=4).in_edges.src.id) == [1, 3, 4]
        assert sorted(v.after(start=1).in_edges.src.id) == [3, 4]
        assert sorted(v.window(0, 4).out_edges.dst.id) == [3]
        assert sorted(v.before(end=3).out_edges.dst.id) == [3]
        assert sorted(v.after(start=1).out_edges.dst.id) == [4]
        assert sorted((e.src.id, e.dst.id) for e in v.window(0, 4).edges) == [
            (1, 2),
            (2, 3),
            (3, 2),
            (4, 2),
        ]
        assert sorted((e.src.id, e.dst.id) for e in v.before(end=4).edges) == [
            (1, 2),
            (2, 3),
            (3, 2),
            (4, 2),
        ]
        assert sorted((e.src.id, e.dst.id) for e in v.after(start=0).edges) == [
            (1, 2),
            (2, 3),
            (2, 4),
            (3, 2),
            (4, 2),
        ]

    check(g)


def test_static_prop_change():
    # with pytest.raises(Exception):
    g = Graph()
    v = g.add_node(0, 1)
    v.add_metadata({"name": "value1"})

    expected_msg = (
        """Exception: Failed to mutate graph\n"""
        """Caused by:\n"""
        """  -> cannot change property for node '1'\n"""
        """  -> cannot mutate static property 'name'\n"""
        """  -> cannot set previous value 'Some(Str("value1"))' to 'Some(Str("value2"))' in position '0'"""
    )

    # with pytest.raises(Exception, match=re.escape(expected_msg)):
    with pytest.raises(Exception):
        v.add_metadata({"name": "value2"})


def test_metadata_update():
    def updates(v):
        v.update_metadata({"prop1": "value1", "prop2": 123})
        assert v.metadata.get("prop1") == "value1" and v.metadata.get("prop2") == 123
        v.update_metadata({"prop1": "value2", "prop2": 345})
        assert v.metadata.get("prop1") == "value2" and v.metadata.get("prop2") == 345

        v.add_metadata({"name": "value1"})
        v.update_metadata({"name": "value2"})
        assert v.metadata.get("name") == "value2"

    g = Graph()
    updates(g)
    updates(g.add_node(0, 1))
    updates(g.add_edge(0, 1, 2))


def test_triplet_count():
    g = Graph()

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})

    @with_disk_graph
    def check(g):
        assert algorithms.triplet_count(g) == 3

    check(g)


def test_global_clustering_coeffficient():
    g = Graph()

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})
    g.add_edge(0, 4, 2, {})
    g.add_edge(0, 4, 1, {})
    g.add_edge(0, 5, 2, {})

    @with_disk_graph
    def check(g):
        assert algorithms.global_clustering_coefficient(g) == 0.5454545454545454
        assert algorithms.global_clustering_coefficient(g.at(0)) == 0.5454545454545454

    check(g)


def test_edge_time_apis():
    g = Graph()

    g.add_edge(1, 1, 2, {"prop2": 10})
    g.add_edge(2, 2, 4, {"prop2": 11})
    g.add_edge(3, 4, 5, {"prop2": 12})
    g.add_edge(4, 1, 5, {"prop2": 13})

    @with_disk_graph
    def check(g):
        v = g.node(1)
        e = g.edge(1, 2)

        for e in e.expanding(1):
            assert e.src.name == "1"
            assert e.dst.name == "2"

        ls = []
        for e in v.edges:
            ls.append(e.src.name)
            ls.append(e.dst.name)

        assert ls == ["1", "2", "1", "5"]

        v = g.node(2)
        ls = []
        for e in v.in_edges:
            ls.append(e.src.name)
            ls.append(e.dst.name)

        assert ls == ["1", "2"]

        ls = []
        for e in v.out_edges:
            ls.append(e.src.name)
            ls.append(e.dst.name)

        assert ls == ["2", "4"]

    check(g)


def test_edge_earliest_latest_time():
    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(1, 1, 3, {})
    g.add_edge(2, 1, 3, {})

    @with_disk_graph
    def check(g):
        assert g.edge(1, 2).earliest_time == 0
        assert g.edge(1, 2).latest_time == 2

        assert list(g.node(1).edges.earliest_time) == [0, 0]
        assert list(g.node(1).edges.latest_time) == [2, 2]
        assert list(g.node(1).at(1).edges.earliest_time) == [1, 1]
        assert list(g.node(1).before(1).edges.earliest_time) == [0, 0]
        assert list(g.node(1).after(1).edges.earliest_time) == [2, 2]
        assert list(g.node(1).at(1).edges.latest_time) == [1, 1]
        assert list(g.node(1).before(1).edges.latest_time) == [0, 0]
        assert list(g.node(1).after(1).edges.latest_time) == [2, 2]

    check(g)


def test_node_earliest_time():
    g = Graph()
    g.add_node(0, 1, {})
    g.add_node(1, 1, {})
    g.add_node(2, 1, {})

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        view = g.at(1)
        assert view.node(1).earliest_time == 1
        assert view.node(1).latest_time == 1

        view = g.after(0)
        assert view.node(1).earliest_time == 1
        assert view.node(1).latest_time == 2

        view = g.before(3)
        assert view.node(1).earliest_time == 0
        assert view.node(1).latest_time == 2

    check(g)


def test_node_history():
    g = Graph()

    g.add_node(1, 1, {})
    g.add_node(2, 1, {})
    g.add_node(3, 1, {})
    g.add_node(4, 1, {})
    g.add_node(8, 1, {})

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        check_arr(g.node(1).history(), [1, 2, 3, 4, 8])
        view = g.window(1, 8)
        check_arr(view.node(1).history(), [1, 2, 3, 4])

    check(g)

    g = Graph()

    g.add_node(4, "Lord Farquaad", {})
    g.add_node(6, "Lord Farquaad", {})
    g.add_node(7, "Lord Farquaad", {})
    g.add_node(8, "Lord Farquaad", {})

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        check_arr(g.node("Lord Farquaad").history(), [4, 6, 7, 8])
        view = g.window(1, 8)
        check_arr(view.node("Lord Farquaad").history(), [4, 6, 7])

    check(g)


def test_edge_history():
    g = Graph()

    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 2)
    g.add_edge(4, 1, 4)

    @with_disk_graph
    def check(g):
        view = g.window(1, 5)
        view2 = g.window(1, 4)

        check_arr(g.edge(1, 2).history(), [1, 3])
        check_arr(view.edge(1, 4).history(), [4])
        check_arr(list(g.edges.history()), [[1, 3], [2], [4]])
        check_arr(list(view2.edges.history()), [[1, 3], [2]])

        old_way = []
        for e in g.edges:
            old_way.append(e.history())
        check_arr(list(g.edges.history()), old_way)

        check_arr(
            g.nodes.edges.history().collect(),
            [
                [[1, 3], [2], [4]],
                [[1, 3]],
                [[2]],
                [[4]],
            ],
        )

        old_way2 = []
        for edges in g.nodes.edges:
            for edge in edges:
                old_way2.append(edge.history())
        new_way = g.nodes.edges.history().collect()
        check_arr([np.array(item) for sublist in new_way for item in sublist], old_way2)

    check(g)


def test_lotr_edge_history():
    g = graph_loader.lotr_graph()

    @with_disk_graph
    def check(g):
        check_arr(
            g.edge("Frodo", "Gandalf").history(),
            [
                329,
                555,
                861,
                1056,
                1130,
                1160,
                1234,
                1241,
                1390,
                1417,
                1656,
                1741,
                1783,
                1785,
                1792,
                1804,
                1809,
                1999,
                2056,
                2254,
                2925,
                2999,
                3703,
                3914,
                4910,
                5620,
                5775,
                6381,
                6531,
                6578,
                6661,
                6757,
                7041,
                7356,
                8183,
                8190,
                8276,
                8459,
                8598,
                8871,
                9098,
                9343,
                9903,
                11189,
                11192,
                11279,
                11365,
                14364,
                21551,
                21706,
                23212,
                26958,
                27060,
                29024,
                30173,
                30737,
                30744,
                31023,
                31052,
                31054,
                31103,
                31445,
                32656,
            ],
        )
        check_arr(g.before(1000).edge("Frodo", "Gandalf").history(), [329, 555, 861])
        check_arr(g.edge("Frodo", "Gandalf").before(1000).history(), [329, 555, 861])
        check_arr(
            g.window(100, 1000).edge("Frodo", "Gandalf").history(), [329, 555, 861]
        )
        check_arr(
            g.edge("Frodo", "Gandalf").window(100, 1000).history(), [329, 555, 861]
        )

    check(g)


# def test_generic_taint_loader():
#     g = graph_loader.stable_coin_graph("/tmp/stablecoin",true, 1)
#
#     start_time = time.time()
#     algorithms.pagerank(g, 20)
#     end_time = time.time()
#
#     print("Time taken (in secs) to run pagerank on stablecoin data", end_time - start_time)
#
#     actual = algorithms.generic_taint(g, 20, 1651105815000, ["0xd30b438df65f4f788563b2b3611bd6059bff4ad9"], [])
#     print(actual)
#     expected = {
#         '0xd30b438df65f4f788563b2b3611bd6059bff4ad9': [(1651105815000, 'start')],
#         '0xda816e2122a8a39b0926bfa84edd3d42477e9efd': [(1651105815000, '0xd30b438df65f4f788563b2b3611bd6059bff4ad9')],
#     }
#
#     assert (actual == expected)


def test_layer():
    g = Graph()

    g.add_edge(0, 1, 2)
    g.add_edge(0, 1, 3, layer="layer1")
    g.add_edge(0, 1, 5, layer="layer1")
    g.add_edge(0, 1, 6, layer="layer1")
    g.add_edge(0, 1, 4, layer="layer2")

    # @with_disk_graph FIXME: no multi layer support
    def check(g):
        assert g.default_layer().count_edges() == 1
        assert g.layers(["layer1"]).count_edges() == 3
        assert g.layers(["layer2"]).count_edges() == 1

        assert g.exclude_layers(["layer1"]).count_edges() == 2
        assert g.exclude_layer("layer1").count_edges() == 2
        assert g.exclude_layers(["layer1", "layer2"]).count_edges() == 1
        assert g.exclude_layer("layer2").count_edges() == 4

        with pytest.raises(
            Exception,
            match=re.escape(
                "Invalid layer: test_layer. Valid layers: _default, layer1, layer2"
            ),
        ):
            g.layers(["test_layer"])

        with pytest.raises(
            Exception,
            match=re.escape(
                "Invalid layer: test_layer. Valid layers: _default, layer1, layer2"
            ),
        ):
            g.edge(1, 2).layers(["test_layer"])

    check(g)


def test_layer_node():
    g = Graph()

    g.add_edge(0, 1, 2, layer="layer1")
    g.add_edge(0, 2, 3, layer="layer2")
    g.add_edge(3, 2, 4, layer="layer1")
    neighbours = g.layers(["layer1", "layer2"]).node(1).neighbours.collect()
    assert sorted(neighbours[0].layers(["layer2"]).edges.id) == [(2, 3)]
    assert sorted(g.layers(["layer2"]).node(neighbours[0].name).edges.id) == [(2, 3)]
    assert sorted(g.layers(["layer1"]).node(neighbours[0].name).edges.id) == [
        (1, 2),
        (2, 4),
    ]
    assert sorted(g.layers(["layer1"]).edges.id) == [(1, 2), (2, 4)]
    assert sorted(g.layers(["layer1", "layer2"]).edges.id) == [(1, 2), (2, 3), (2, 4)]


def test_rolling_as_iterable():
    g = Graph()

    g.add_node(1, 1)
    g.add_node(4, 4)

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        rolling = g.rolling(1)

        # a normal operation is reusing the object returned by rolling twice, to get both results and an index.
        # So the following should work fine:
        n_nodes = [w.count_nodes() for w in rolling]
        time_index = [w.start for w in rolling]

        assert n_nodes == [1, 0, 0, 1]
        assert time_index == [1, 2, 3, 4]

    check(g)


def test_layer_name():
    g = Graph()

    g.add_edge(0, 0, 1)
    g.add_edge(0, 0, 2, layer="awesome layer")

    assert g.edge(0, 1).layer_names == ["_default"]
    assert g.edge(0, 2).layer_names == ["awesome layer"]

    error_msg = (
        "The layer_name function is only available once an edge has been exploded via .explode_layers() or .explode(). "
        "If you want to retrieve the layers for this edge you can use .layer_names"
    )
    with pytest.raises(Exception) as e:
        g.edges.layer_name()
    assert str(e.value) == error_msg

    assert list(g.edges.explode().layer_name) == ["_default", "awesome layer"]
    assert list(g.edges.explode_layers().layer_name) == ["_default", "awesome layer"]

    with pytest.raises(Exception) as e:
        g.edge(0, 2).layer_name()
    assert str(e.value) == error_msg

    assert list(g.edge(0, 2).explode().layer_name) == ["awesome layer"]
    assert list(g.edge(0, 2).explode_layers().layer_name) == ["awesome layer"]

    with pytest.raises(Exception) as e:
        g.nodes.neighbours.edges.layer_name()
    assert str(e.value) == error_msg

    assert [
        list(iterator) for iterator in g.nodes.neighbours.edges.explode().layer_name
    ] == [
        ["_default", "awesome layer"],
        ["_default", "awesome layer"],
        ["_default", "awesome layer"],
    ]
    assert [
        list(iterator)
        for iterator in g.nodes.neighbours.edges.explode_layers().layer_name
    ] == [
        ["_default", "awesome layer"],
        ["_default", "awesome layer"],
        ["_default", "awesome layer"],
    ]


def test_time():
    g = Graph()
    g.add_metadata({"name": "graph"})

    g.add_edge(0, 0, 1)
    g.add_edge(0, 0, 2)
    g.add_edge(1, 0, 2)

    @with_disk_graph
    def check(g):
        error_msg = (
            "The time function is only available once an edge has been exploded via .explode(). "
            "You may want to retrieve the history for this edge via .history(), or the earliest/latest time via earliest_time or latest_time"
        )
        # with pytest.raises(Exception) as e:
        #     g.edges.time()
        # assert str(e.value) == error_msg

        assert list(g.edges.explode().time) == [0, 0, 1]

        # with pytest.raises(Exception) as e:
        #     g.edge(0, 2).time()
        # assert str(e.value) == error_msg

        assert list(g.edge(0, 2).explode().time) == [0, 1]

        # with pytest.raises(Exception) as e:
        #     g.nodes.neighbours.edges.time()
        # assert str(e.value) == error_msg

        assert [
            list(iterator) for iterator in g.nodes.neighbours.edges.explode().time
        ] == [
            [0, 0, 1],
            [0, 0, 1],
            [0, 0, 1],
        ]

    check(g)


def test_window_size():
    g = Graph()
    g.add_node(1, 1)
    g.add_node(4, 4)

    @with_disk_graph
    def check(g):
        assert g.window_size is None
        assert g.window(1, 5).window_size == 4

    check(g)


def test_time_index():
    g = Graph()

    @with_disk_graph
    def check(g):
        w = g.window("2020-01-01", "2020-01-03")
        rolling = w.rolling("1 day")
        time_index = rolling.time_index()
        assert list(time_index) == [
            datetime(2020, 1, 1, 23, 59, 59, 999000, tzinfo=utc),
            datetime(2020, 1, 2, 23, 59, 59, 999000, tzinfo=utc),
        ]

        w = g.window(1, 3)
        rolling = w.rolling(1)
        time_index = rolling.time_index()
        assert list(time_index) == [1, 2]

        w = g.window(0, 100)
        rolling = w.rolling(50)
        time_index = rolling.time_index(center=True)
        assert list(time_index) == [25, 75]

    check(g)


def test_datetime_props():
    g = Graph()

    # @with_disk_graph FIXME support for DateTime properties
    def check(g):
        dt1 = datetime(2020, 1, 1, 23, 59, 59, 999000)
        g.add_node(0, 0, {"time": dt1})
        assert g.node(0).properties.get("time") == dt1

        dt2 = datetime(2020, 1, 1, 23, 59, 59, 999999)
        g.add_node(0, 1, {"time": dt2})
        assert g.node(1).properties.get("time") == dt2

    check(g)


def test_date_time():
    g = Graph()

    g.add_edge("2014-02-02", 1, 2)
    g.add_edge("2014-02-03", 1, 3)
    g.add_edge("2014-02-04", 1, 4)
    g.add_edge("2014-02-05", 1, 2)

    @with_disk_graph
    def check(g):
        assert g.earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert g.latest_date_time == datetime(2014, 2, 5, 0, 0, tzinfo=utc)

        e = g.edge(1, 3)
        exploded_edges = []
        for edge in e.explode():
            exploded_edges.append(edge.date_time)
        assert exploded_edges == [datetime(2014, 2, 3, tzinfo=utc)]
        assert g.edge(1, 2).earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert g.edge(1, 2).latest_date_time == datetime(2014, 2, 5, 0, 0, tzinfo=utc)

        assert g.node(1).earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert g.node(1).latest_date_time == datetime(2014, 2, 5, 0, 0, tzinfo=utc)

    check(g)


def test_float_ts():
    g = Graph()
    g.add_node(1e-3, 1)
    assert g.node(1).earliest_time == 1

    with pytest.raises(RuntimeError):
        # don't silently loose precision
        g.add_node(1e-4, 2)

    g.add_node(0.0, 3)
    assert g.node(3).earliest_time == 0

    g.add_node(1001 / 1000, 4)
    assert g.node(4).earliest_time == 1001


def test_date_time_window():
    g = Graph()

    g.add_edge("2014-02-02", 1, 2)
    g.add_edge("2014-02-03", 1, 3)
    g.add_edge("2014-02-04", 1, 4)
    g.add_edge("2014-02-05", 1, 2)
    g.add_edge("2014-02-06", 1, 2)

    @with_disk_graph
    def check(g):
        view = g.window("2014-02-02", "2014-02-04")
        view2 = g.window("2014-02-02", "2014-02-05")

        assert view.start_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view.end_date_time == datetime(2014, 2, 4, 0, 0, tzinfo=utc)

        assert view.earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view.latest_date_time == datetime(2014, 2, 3, 0, 0, tzinfo=utc)

        assert view2.edge(1, 2).start_date_time == datetime(
            2014, 2, 2, 0, 0, tzinfo=utc
        )
        assert view2.edge(1, 2).end_date_time == datetime(2014, 2, 5, 0, 0, tzinfo=utc)

        assert view.node(1).earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view.node(1).latest_date_time == datetime(2014, 2, 3, 0, 0, tzinfo=utc)

        e = view.edge(1, 2)
        exploded_edges = []
        for edge in e.explode():
            exploded_edges.append(edge.date_time)
        assert exploded_edges == [datetime(2014, 2, 2, tzinfo=utc)]

    check(g)


def test_datetime_add_node():
    g = Graph()
    g.add_node(datetime(2014, 2, 2), 1)
    g.add_node(datetime(2014, 2, 3), 2)
    g.add_node(datetime(2014, 2, 4), 2)
    g.add_node(datetime(2014, 2, 5), 4)
    g.add_node(datetime(2014, 2, 6), 5)

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        view = g.window("2014-02-02", "2014-02-04")
        view2 = g.window("2014-02-02", "2014-02-05")

        assert view.start_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view.end_date_time == datetime(2014, 2, 4, 0, 0, tzinfo=utc)

        assert view2.earliest_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view2.latest_date_time == datetime(2014, 2, 4, 0, 0, tzinfo=utc)

        assert view2.node(1).start_date_time == datetime(2014, 2, 2, 0, 0, tzinfo=utc)
        assert view2.node(1).end_date_time == datetime(2014, 2, 5, 0, 0, tzinfo=utc)

        assert view.node(2).earliest_date_time == datetime(2014, 2, 3, 0, 0, tzinfo=utc)
        assert view.node(2).latest_date_time == datetime(2014, 2, 3, 0, 0, tzinfo=utc)

    check(g)


def test_datetime_with_timezone():
    from raphtory import Graph
    import pytz

    g = Graph()
    # testing zones east and west of UK
    timezones = [
        "Asia/Kolkata",
        "America/New_York",
        "US/Central",
        "Europe/London",
        "Australia/Sydney",
        "Africa/Johannesburg",
    ]
    results = [
        datetime(2024, 1, 5, 1, 0, tzinfo=utc),
        datetime(2024, 1, 5, 6, 30, tzinfo=utc),
        datetime(2024, 1, 5, 10, 0, tzinfo=utc),
        datetime(2024, 1, 5, 12, 0, tzinfo=utc),
        datetime(2024, 1, 5, 17, 0, tzinfo=utc),
        datetime(2024, 1, 5, 18, 0, tzinfo=utc),
    ]

    for tz in timezones:
        timezone = pytz.timezone(tz)
        naive_datetime = datetime(2024, 1, 5, 12, 0, 0)
        localized_datetime = timezone.localize(naive_datetime)
        g.add_node(localized_datetime, 1)

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph
    def check(g):
        assert g.node(1).history_date_time() == results

    check(g)


def test_equivalent_nodes_edges_and_sets():
    g = Graph()
    g.add_node(1, 1)
    g.add_node(1, 2)
    g.add_node(1, 3)

    g.add_edge(1, 1, 2)
    g.add_edge(1, 2, 3)

    @with_disk_graph
    def check(g):
        assert g.node(1) == g.node(1)
        assert list(g.node(1).neighbours)[0] == list(g.node(3).neighbours)[0]
        assert set(g.node(1).neighbours) == set(g.node(3).neighbours)
        assert set(g.node(1).out_edges) == set(g.node(2).in_edges)

        assert g.edge(1, 1) == g.edge(1, 1)

    check(g)


def test_subgraph():
    g = create_graph()

    @with_disk_graph
    def check(g):
        empty_graph = g.subgraph([])
        assert empty_graph.nodes.collect() == []

        node1 = g.nodes[1]
        subgraph = g.subgraph([node1])
        assert subgraph.nodes.collect() == [node1]

        subgraph_from_str = g.subgraph(["1"])
        assert subgraph_from_str.nodes.collect() == [node1]

        subgraph_from_int = g.subgraph([1])
        assert subgraph_from_int.nodes.collect() == [node1]

        mg = subgraph.materialize()
        assert mg.nodes.collect()[0].properties["type"] == "wallet"
        assert mg.nodes.collect()[0].name == "1"

        props = {"prop 4": 11, "prop 5": "world", "prop 6": False}
        mg.add_properties(1, props)

        props = {"prop 1": 1, "prop 2": "hi", "prop 3": True}
        mg.add_metadata(props)
        x = mg.properties.keys()
        x.sort()
        assert x == ["prop 4", "prop 5", "prop 6"]
        x = mg.metadata.keys()
        x.sort()
        assert x == ["prop 1", "prop 2", "prop 3"]

    check(g)


def test_exclude_nodes():
    g = create_graph()

    @with_disk_graph
    def check(g):
        exclude_nodes = g.exclude_nodes([1])
        assert exclude_nodes.nodes.id.collect() == [2, 3]

    check(g)


def test_nbr():
    g = create_graph()

    @with_disk_graph
    def check(g):
        r = [e.nbr.name for e in g.edges]
        r.sort()
        assert r == ["1", "1", "2", "2", "3"]

    check(g)


def test_materialize_graph():
    g = Graph()

    edges = [(1, 1, 2), (2, 1, 3), (-1, 2, 1), (0, 1, 1), (7, 3, 2), (1, 1, 1)]

    g.add_node(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_node(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_node(6, 3, {"type": "wallet", "cost": 76.0})
    g.add_node(6, 4).add_metadata({"abc": "xyz"})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    g.add_edge(8, 2, 4)

    sprop = {"sprop 1": "kaggle", "sprop 2": True}
    g.add_metadata(sprop)
    assert g.metadata == sprop

    # @with_disk_graph FIXME: need special handling for nodes additions from Graph, support for metadata on edges
    def check(g):
        def check_g_inner(mg):
            assert mg.node(1).properties.get("type") == "wallet"
            assert mg.node(4).metadata == {"abc": "xyz"}
            assert mg.node(4).metadata.get("abc") == "xyz"
            check_arr(mg.node(1).history(), [-1, 0, 1, 2])
            check_arr(mg.node(4).history(), [6, 8])
            assert mg.nodes.id.collect() == [1, 2, 3, 4]
            assert set(mg.edges.id) == {(1, 1), (1, 2), (1, 3), (2, 1), (3, 2), (2, 4)}
            assert g.nodes.id.collect() == mg.nodes.id.collect()
            assert set(g.edges.id) == set(mg.edges.id)
            assert mg.node(1).metadata == {}
            assert mg.node(4).metadata == {"abc": "xyz"}
            assert g.edge(1, 2).id == (1, 2)
            assert mg.edge(1, 2).id == (1, 2)
            assert mg.has_edge(1, 2)
            assert g.has_edge(1, 2)
            assert mg.has_edge(2, 1)
            assert g.has_edge(2, 1)

        check_g_inner(g)

        mg = g.materialize()

        check_g_inner(mg)

        sprop2 = {"sprop 3": 11, "sprop 4": 10}
        mg.add_metadata(sprop2)
        sprop.update(sprop2)
        assert mg.metadata == sprop

    check(g)


def test_deletions():
    g = create_graph_with_deletions()

    # @with_disk_graph FIXME: add support for edge deletions
    def check(g):
        deleted_edge = g.edge(edges[0][1], edges[0][2])
        for e in edges:
            assert g.at(e[0]).has_edge(e[1], e[2])
            assert g.after(e[0]).has_edge(e[1], e[2])

        for e in edges[:-1]:
            # last update is an existing edge
            assert not g.before(e[0]).has_edge(e[1], e[2])

        # deleted at window start
        assert deleted_edge.window(10, 20).is_deleted()
        assert not deleted_edge.window(10, 20).is_valid()
        assert deleted_edge.window(10, 20).earliest_time is None
        assert deleted_edge.window(10, 20).latest_time is None

        # deleted before window start
        assert deleted_edge.window(15, 20).is_deleted()
        assert not deleted_edge.window(15, 20).is_valid()
        assert deleted_edge.window(15, 20).earliest_time is None
        assert deleted_edge.window(15, 20).latest_time is None

        # deleted in window
        assert deleted_edge.window(5, 20).is_deleted()
        assert not deleted_edge.window(5, 20).is_valid()
        assert deleted_edge.window(5, 20).earliest_time == 5
        assert deleted_edge.window(5, 20).latest_time == 10

        # check deleted edge is gone at 10
        assert not g.after(start=10).has_edge(edges[0][1], edges[0][2])
        assert not g.at(10).has_edge(edges[0][1], edges[0][2])
        assert g.before(10).has_edge(edges[0][1], edges[0][2])

        # check not deleted edges are still there
        for e in edges[1:]:
            assert g.after(start=10).has_edge(e[1], e[2])

        assert list(deleted_edge.explode().latest_time) == [10]
        assert list(deleted_edge.explode().earliest_time) == [edges[0][0]]

        # check rolling and expanding behaviour
        assert not list(g.before(1).node(1).after(1).rolling(1))
        assert not list(g.after(0).edge(1, 1).before(1).expanding(1))

    check(g)


def test_edge_layer():
    g = Graph()
    g.add_edge(1, 1, 2, layer="layer 1").add_metadata({"test_prop": "test_val"})
    g.add_edge(1, 2, 3, layer="layer 2").add_metadata({"test_prop": "test_val 2"})

    # @with_disk_graph #FIXME: add support for edge metadata
    def check(g):
        assert g.edges.metadata.get("test_prop") == [
            {"layer 1": "test_val"},
            {"layer 2": "test_val 2"},
        ]

    check(g)


def test_layers_earliest_time():
    g = Graph()
    e = g.add_edge(1, 1, 2, layer="test")

    # @with_disk_graph # FIXME: add support for multiple layers
    def check(g):
        e = g.edge(1, 2)
        assert e.earliest_time == 1

    check(g)


def test_edge_explode_layers():
    g = Graph()
    g.add_edge(1, 1, 2, {"layer": 1}, layer="1")
    g.add_edge(1, 1, 2, {"layer": 2}, layer="2")
    g.add_edge(1, 2, 1, {"layer": 1}, layer="1")
    g.add_edge(1, 2, 1, {"layer": 2}, layer="2")

    # @with_disk_graph # FIXME: add edge multi layer support
    def check(g):
        layered_edges = g.edge(1, 2).explode_layers()
        e_layers = [ee.layer_names for ee in layered_edges]
        e_layer_prop = [[str(ee.properties["layer"])] for ee in layered_edges]
        assert e_layers == e_layer_prop

        nested_layered_edges = g.nodes.out_edges.explode_layers()
        e_layers = [[ee.layer_names for ee in edges] for edges in nested_layered_edges]
        e_layer_prop = [
            [[str(ee.properties["layer"])] for ee in layered_edges]
            for layered_edges in nested_layered_edges
        ]
        assert e_layers == e_layer_prop

        nested_layered_edges = g.nodes.out_neighbours.out_edges.explode_layers()
        e_layers = [
            [ee.layer_names for ee in layered_edges]
            for layered_edges in nested_layered_edges
        ]
        e_layer_prop = [
            [[str(ee.properties["layer"])] for ee in layered_edges]
            for layered_edges in nested_layered_edges
        ]
        assert e_layers == e_layer_prop

    check(g)


def test_starend_edges():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)

    @with_disk_graph
    def check(g):
        old_latest_time_way = []
        for e in g.edges:
            old_latest_time_way.append(e.latest_time)

        assert old_latest_time_way == list(g.edges.latest_time)

        old_earliest_time_way = []
        for e in g.edges:
            old_earliest_time_way.append(e.earliest_time)
        assert old_earliest_time_way == list(g.edges.earliest_time)

        old_latest_time_nested_way = []
        old_earliest_time_nested_way = []
        for edges in g.nodes.edges:
            for edge in edges:
                old_latest_time_nested_way.append(edge.latest_time)
                old_earliest_time_nested_way.append(edge.earliest_time)

        assert old_latest_time_nested_way == [
            item for sublist in g.nodes.edges.latest_time.collect() for item in sublist
        ]
        assert old_earliest_time_nested_way == [
            item
            for sublist in g.nodes.edges.earliest_time.collect()
            for item in sublist
        ]
        gw = g.window(1, 3)
        assert gw.edges.start == gw.start
        assert gw.edges.end == gw.end
        assert gw.nodes.edges.start == gw.start
        assert gw.nodes.edges.end == gw.end

    check(g)


def test_date_time_edges():
    g = Graph()

    g.add_edge("2014-02-02", 1, 2)
    g.add_edge("2014-02-03", 1, 3)
    g.add_edge("2014-02-04", 1, 4)
    g.add_edge("2014-02-05", 1, 2)

    @with_disk_graph
    def check(g):
        old_date_way = []
        for edges in g.nodes.edges:
            for edge in edges:
                old_date_way.append(edge.date_time)

        assert old_date_way == [
            item for sublist in g.nodes.edges.date_time.collect() for item in sublist
        ]
        gw = g.window("2014-02-02", "2014-02-05")
        assert gw.edges.start_date_time == gw.start_date_time
        assert gw.edges.end_date_time == gw.end_date_time
        assert gw.nodes.edges.start_date_time == gw.start_date_time
        assert gw.nodes.edges.end_date_time == gw.end_date_time

    check(g)


def test_layer_edges():
    g = Graph()
    g.add_edge(1, 1, 2, layer="layer 1")
    g.add_edge(2, 1, 2, layer="layer 2")
    g.add_edge(3, 1, 2, layer="layer 3")
    layer_names = ["layer 1", "layer 2", "layer 3"]

    @with_disk_graph
    def check(g):
        old_layer_way = []
        for e in g.edges:
            old_layer_way.append(e.layer("layer 1"))
        assert old_layer_way == list(g.edges.layer("layer 1"))

        old_layers_way = []
        for e in g.edges:
            old_layers_way.append(e.layers(layer_names))
        assert old_layers_way == list(g.edges.layers(layer_names))

    check(g)


def test_window_edges():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(4, 1, 2)

    @with_disk_graph
    def check(g):
        old_window_way = []
        for e in g.edges:
            old_window_way.append(e.window(2, 3))
        assert old_window_way == list(g.edges.window(2, 3))

    check(g)


def test_weird_windows():
    g = Graph()
    g.add_edge(1, 1, 2)

    @with_disk_graph
    def check(g):
        with pytest.raises(
            Exception,
            match="'ddd' is not a valid datetime, valid formats are RFC3339, RFC2822, %Y-%m-%d, %Y-%m-%dT%H:%M:%S%.3f, %Y-%m-%dT%H:%M:%S%, %Y-%m-%d %H:%M:%S%.3f and %Y-%m-%d %H:%M:%S%",
        ):
            g.window("ddd", "aaa")

    check(g)


def test_at_edges():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(4, 1, 2)

    @with_disk_graph
    def check(g):
        old_at_way = []
        for e in g.edges:
            old_at_way.append(e.at(2))
        assert old_at_way == list(g.edges.at(2))

    check(g)


def test_snapshot():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)

    for time in range(0, 4):
        assert g.before(time + 1) == g.snapshot_at(time)
    assert g == g.snapshot_latest()

    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.delete_edge(3, 1, 2)

    for time in range(0, 5):
        assert g.at(time) == g.snapshot_at(time)
    assert g.latest() == g.snapshot_latest()


def test_one_hop_filter_reset():
    g = Graph()
    g.add_edge(0, 1, 2, {"layer": 1}, "1")
    g.add_edge(1, 1, 3, {"layer": 1}, "1")
    g.add_edge(1, 2, 3, {"layer": 2}, "2")
    g.add_edge(2, 3, 4, {"layer": 2}, "2")
    g.add_edge(0, 1, 3, {"layer": 2}, "2")

    @with_disk_graph
    def check(g):
        v = g.node(1)

        # filtering resets on neighbours
        out_out = v.at(0).layer("1").out_neighbours.layer("2").out_neighbours.id
        assert out_out == [3]

        out_out = (
            v.at(0)
            .layer("1")
            .out_neighbours.layer("2")
            .out_edges.properties.get("layer")
        )
        assert out_out == [2]

        out_out = v.at(0).out_neighbours.after(1).out_neighbours.id
        assert out_out == [4]

        earliest_time = v.at(0).out_neighbours.after(1).out_edges.earliest_time.min()
        assert earliest_time == 2

        # filter applies to edges
        layers = set(v.layer("1").edges.explode_layers().layer_name)
        assert layers == {"1"}

        # dst and src on edge reset the filter
        degrees = v.at(0).layer("1").edges.dst.out_degree()
        assert degrees == [1]

        # graph level filter is preserved
        out_out_2 = (
            g.at(0).node(1).layer("1").out_neighbours.layer("2").out_neighbours.id
        )
        assert len(out_out_2) == 0

    check(g)


def test_type_filter():
    g = Graph()
    g.add_node(1, 1, node_type="wallet")
    g.add_node(1, 2, node_type="timer")
    g.add_node(1, 3, node_type="timer")
    g.add_node(1, 4, node_type="wallet")

    @with_disk_graph
    def check(g):
        assert [node.name for node in g.nodes.type_filter(["wallet"])] == ["1", "4"]
        assert g.subgraph_node_types(["timer"]).nodes.name.collect() == ["2", "3"]

    check(g)

    g = PersistentGraph()
    g.add_node(1, 1, node_type="wallet")
    g.add_node(2, 2, node_type="timer")
    g.add_node(3, 3, node_type="timer")
    g.add_node(4, 4, node_type="wallet")

    # @with_disk_graph # FIXME PersistentGraph cannot be used with with_disk_graph
    def check(g):
        assert [node.name for node in g.nodes.type_filter(["wallet"])] == ["1", "4"]
        assert g.subgraph_node_types(["timer"]).nodes.name.collect() == ["2", "3"]

        subgraph = g.subgraph([1, 2, 3])
        assert [node.name for node in subgraph.nodes.type_filter(["wallet"])] == ["1"]
        assert subgraph.subgraph_node_types(["timer"]).nodes.name.collect() == [
            "2",
            "3",
        ]

        w = g.window(1, 4)
        assert [node.name for node in w.nodes.type_filter(["wallet"])] == ["1"]
        assert w.subgraph_node_types(["timer"]).nodes.name.collect() == ["2", "3"]

    check(g)

    g = Graph()
    g.add_node(1, 1, node_type="wallet")
    g.add_node(2, 2, node_type="timer")
    g.add_node(3, 3, node_type="timer")
    g.add_node(4, 4, node_type="counter")
    g.add_edge(1, 1, 2, layer="layer1")
    g.add_edge(2, 2, 3, layer="layer1")
    g.add_edge(3, 2, 4, layer="layer2")

    @with_disk_graph
    def check(g):
        layer = g.layers(["layer1"])
        assert [node.name for node in layer.nodes.type_filter(["wallet"])] == ["1"]
        assert layer.subgraph_node_types(["timer"]).nodes.name.collect() == ["2", "3"]

    check(g)

    g = Graph()
    g.add_node(1, 1, node_type="a")
    g.add_node(1, 2, node_type="b")
    g.add_node(1, 3, node_type="b")
    g.add_node(1, 4, node_type="a")
    g.add_node(1, 5, node_type="c")
    g.add_node(1, 6, node_type="e")
    g.add_node(1, 7)
    g.add_node(1, 8)
    g.add_node(1, 9)
    g.add_edge(2, 1, 2, layer="a")
    g.add_edge(2, 3, 2, layer="a")
    g.add_edge(2, 2, 4, layer="a")
    g.add_edge(2, 4, 5, layer="a")
    g.add_edge(2, 4, 5, layer="a")
    g.add_edge(2, 5, 6, layer="a")
    g.add_edge(2, 3, 6, layer="a")

    # @with_disk_graph # FIXME: add support for type_filters + layers support on edges
    def check(g):
        assert g.nodes.type_filter([""]).name.collect() == ["7", "8", "9"]

        assert g.nodes.type_filter(["a"]).name.collect() == ["1", "4"]
        assert g.nodes.type_filter(["a", "c"]).name.collect() == ["1", "4", "5"]
        assert g.nodes.type_filter(["a"]).neighbours.name.collect() == [
            ["2"],
            ["2", "5"],
        ]

        assert g.nodes.degree().collect() == [1, 3, 2, 2, 2, 2, 0, 0, 0]
        assert g.nodes.type_filter(["a"]).degree().collect() == [1, 2]
        assert g.nodes.type_filter(["d"]).degree().collect() == []
        assert g.nodes.type_filter([]).name.collect() == []

        assert len(g.nodes) == 9
        assert len(g.nodes.type_filter(["b"])) == 2
        assert len(g.nodes.type_filter(["d"])) == 0

        assert g.nodes.type_filter(["d"]).neighbours.name.collect() == []
        assert g.nodes.type_filter(["a"]).neighbours.name.collect() == [
            ["2"],
            ["2", "5"],
        ]
        assert g.nodes.type_filter(["a", "c"]).neighbours.name.collect() == [
            ["2"],
            ["2", "5"],
            ["4", "6"],
        ]

        assert g.nodes.type_filter(["a"]).neighbours.type_filter(
            ["c"]
        ).name.collect() == [
            [],
            ["5"],
        ]
        assert g.nodes.type_filter(["a"]).neighbours.type_filter([]).name.collect() == [
            [],
            [],
        ]
        assert g.nodes.type_filter(["a"]).neighbours.type_filter(
            ["b", "c"]
        ).name.collect() == [["2"], ["2", "5"]]
        assert g.nodes.type_filter(["a"]).neighbours.type_filter(
            ["d"]
        ).name.collect() == [
            [],
            [],
        ]
        assert g.nodes.type_filter(["a"]).neighbours.neighbours.name.collect() == [
            ["1", "3", "4"],
            ["1", "3", "4", "4", "6"],
        ]
        assert g.nodes.type_filter(["a"]).neighbours.type_filter(
            ["c"]
        ).neighbours.name.collect() == [[], ["4", "6"]]
        assert g.nodes.type_filter(["a"]).neighbours.type_filter(
            ["d"]
        ).neighbours.name.collect() == [[], []]

        assert g.node("2").neighbours.type_filter(["b"]).name.collect() == ["3"]
        assert g.node("2").neighbours.type_filter(["d"]).name.collect() == []
        assert g.node("2").neighbours.type_filter([]).name.collect() == []
        assert g.node("2").neighbours.type_filter(["c", "a"]).name.collect() == [
            "1",
            "4",
        ]
        assert g.node("2").neighbours.type_filter(["c"]).neighbours.name.collect() == []
        assert g.node("2").neighbours.neighbours.name.collect() == [
            "2",
            "2",
            "6",
            "2",
            "5",
        ]

    check(g)


def test_time_exploded_edges():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)
    g.add_edge(4, 1, 3)

    @with_disk_graph
    def check(g):
        time = []
        for e in g.edges.explode():
            time.append(e.time)
        assert list(g.edges.explode().time) == time

        date_time = []
        for e in g.edges.explode():
            date_time.append(e.date_time)
        assert list(g.edges.explode().date_time) == date_time

        time_nested = []
        for edges in g.nodes.edges.explode():
            for edge in edges:
                time_nested.append(edge.time)
        assert [
            item
            for sublist in g.nodes.edges.explode().time.collect()
            for item in sublist
        ] == time_nested

        date_time_nested = []
        for edges in g.nodes.edges.explode():
            for edge in edges:
                date_time_nested.append(edge.date_time)
        assert [
            item
            for sublist in g.nodes.edges.explode().date_time.collect()
            for item in sublist
        ] == date_time_nested

    check(g)


def test_leading_zeroes_ids():
    g = Graph()
    g.add_node(0, "1")
    g.add_node(0, "01")
    g.add_node(0, "001")
    g.add_node(0, "0001")

    @with_disk_graph
    def check(g):
        assert g.count_nodes() == 4
        assert g.nodes.name.collect() == ["1", "01", "001", "0001"]

    check(g)

    g = Graph()
    g.add_node(0, 0)
    g.add_node(1, 0)

    # @with_disk_graph # FIXME: need special handling for nodes additions from Graph
    def check(g):
        check_arr(g.node(0).history(), [0, 1])
        check_arr(g.node("0").history(), [0, 1])
        assert g.nodes.name.collect() == ["0"]

    check(g)


def test_node_types():
    g = Graph()
    a = g.add_node(0, "A", None, None)
    b = g.add_node(0, "B", None, "BTYPE")

    @with_disk_graph
    def check(g):
        assert a.node_type == None
        assert b.node_type == "BTYPE"
        assert set(g.nodes.node_type) == {None, "BTYPE"}

    check(g)


def test_node_types_change():
    g = Graph()
    a = g.add_node(0, "A", None, None)
    assert a.node_type == None
    a.set_node_type("YO")
    assert a.node_type == "YO"


def test_persistent_event_graphs():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 2, 3)
    g.add_edge(3, 1, 3)
    pg = g.persistent_graph()
    pg.delete_edge(4, 1, 3)


def test_is_self_loop():
    g = Graph()
    g.add_node(0, "A", None, None)
    e = g.add_edge(0, "A", "A", None, None)
    assert e.is_self_loop()
    g.add_node(0, "B", None, None)
    ee = g.add_edge(0, "A", "B", None, None)
    assert not ee.is_self_loop()


def test_NaN_NaT_as_properties():
    now = datetime.now()
    data = {
        "floats": [np.nan, None, 2.4, None, None, None],
        "time": [10, 20, 30, 40, 50, 60],
        "id": [101, 102, 103, 104, 105, 106],
        "datetime": [
            now,
            now,
            np.datetime64("NaT"),
            now,
            now,
            now,
        ],  # Hardcoded datetime
    }

    df = pd.DataFrame(data)
    g = Graph()
    g.load_nodes_from_pandas(time="time", id="id", df=df, properties=["floats"])

    @with_disk_graph
    def check(g):
        assert g.node(103).properties.temporal.get("floats").items() == [(30, 2.4)]
        assert g.node(101).properties.temporal.get("floats") == []

    check(g)


def test_unique_temporal_properties():
    g = Graph()
    g.add_properties(1, {"name": "tarzan"})
    g.add_properties(2, {"name": "tarzan2"})
    g.add_properties(3, {"name": "tarzan2"})
    g.add_properties(2, {"salary": "1000"})
    g.add_metadata({"type": "character"})
    g.add_edge(1, 1, 2, properties={"status": "open"})
    g.add_edge(2, 1, 2, properties={"status": "open"})
    g.add_edge(3, 1, 2, properties={"status": "review"})
    g.add_edge(4, 1, 2, properties={"status": "open"})
    g.add_edge(5, 1, 2, properties={"status": "in-progress"})
    g.add_edge(10, 1, 2, properties={"status": "in-progress"})
    g.add_edge(6, 1, 2)
    g.add_node(1, 3, {"name": "avatar1"})
    g.add_node(2, 3, {"name": "avatar2"})
    g.add_node(3, 3, {"name": "avatar2"})

    # @with_disk_graph # FIXME need disk graph to support temporal properties
    def check(g):
        assert g.edge(1, 2).properties.temporal.get("status").ordered_dedupe(True) == [
            (2, "open"),
            (3, "review"),
            (4, "open"),
            (10, "in-progress"),
        ]
        assert g.edge(1, 2).properties.temporal.get("status").ordered_dedupe(False) == [
            (1, "open"),
            (3, "review"),
            (4, "open"),
            (5, "in-progress"),
        ]
        assert g.properties.temporal.get("name").ordered_dedupe(True) == [
            (1, "tarzan"),
            (3, "tarzan2"),
        ]
        assert g.properties.temporal.get("name").ordered_dedupe(False) == [
            (1, "tarzan"),
            (2, "tarzan2"),
        ]
        assert g.node(3).properties.temporal.get("name").ordered_dedupe(True) == [
            (1, "avatar1"),
            (3, "avatar2"),
        ]
        assert g.node(3).properties.temporal.get("name").ordered_dedupe(False) == [
            (1, "avatar1"),
            (2, "avatar2"),
        ]

    check(g)

    g.add_node(4, 3, {"i64": 1})
    g.add_node(5, 3, {"i64": 1})
    g.add_node(6, 3, {"i64": 5})
    g.add_node(7, 3, {"f64": 1.2})
    g.add_node(8, 3, {"f64": 1.3})
    g.add_node(9, 3, {"bool": True})
    g.add_node(10, 3, {"bool": True})
    g.add_node(11, 3, {"bool": False})
    g.add_node(12, 3, {"list": [1, 2, 3]})
    g.add_node(13, 3, {"list": [1, 2, 3]})
    g.add_node(14, 3, {"list": [2, 3]})
    datetime_obj = datetime.strptime("2021-01-01 12:32:00", "%Y-%m-%d %H:%M:%S")
    datetime_obj2 = datetime.strptime("2021-01-02 12:32:00", "%Y-%m-%d %H:%M:%S")
    g.add_node(15, 3, {"date": datetime_obj})
    g.add_node(16, 3, {"date": datetime_obj})
    g.add_node(17, 3, {"date": datetime_obj2})
    g.add_node(18, 3, {"map": {"name": "bob", "value list": [1, 2, 3]}})
    g.add_node(19, 3, {"map": {"name": "bob", "value list": [1, 2]}})

    # @with_disk_graph # FIXME List, Map and NDTime properties are not supported
    def check(g):
        assert list(g.edge(1, 2).properties.temporal.get("status")) == [
            (1, "open"),
            (2, "open"),
            (3, "review"),
            (4, "open"),
            (5, "in-progress"),
            (10, "in-progress"),
        ]
        assert sorted(g.edge(1, 2).properties.temporal.get("status").unique()) == [
            "in-progress",
            "open",
            "review",
        ]
        assert list(g.properties.temporal.get("name")) == [
            (1, "tarzan"),
            (2, "tarzan2"),
            (3, "tarzan2"),
        ]
        assert sorted(g.properties.temporal.get("name").unique()) == [
            "tarzan",
            "tarzan2",
        ]
        assert list(g.node(3).properties.temporal.get("name")) == [
            (1, "avatar1"),
            (2, "avatar2"),
            (3, "avatar2"),
        ]
        assert sorted(g.node(3).properties.temporal.get("name").unique()) == [
            "avatar1",
            "avatar2",
        ]
        assert sorted(g.node(3).properties.temporal.get("i64").unique()) == [1, 5]
        assert sorted(g.node(3).properties.temporal.get("f64").unique()) == [1.2, 1.3]
        assert sorted(g.node(3).properties.temporal.get("bool").unique()) == [
            False,
            True,
        ]
        assert sorted(g.node(3).properties.temporal.get("list").unique()) == [
            [1, 2, 3],
            [2, 3],
        ]
        assert sorted(g.node(3).properties.temporal.get("date").unique()) == [
            datetime_obj,
            datetime_obj2,
        ]
        actual_list = g.node(3).properties.temporal.get("map").unique()
        expected_list = [
            {"name": "bob", "value list": [1, 2]},
            {"name": "bob", "value list": [1, 2, 3]},
        ]
        sorted_actual_list = sorted(
            actual_list, key=lambda d: (d["name"], tuple(d["value list"]))
        )
        sorted_expected_list = sorted(
            expected_list, key=lambda d: (d["name"], tuple(d["value list"]))
        )
        assert sorted_actual_list == sorted_expected_list

    check(g)


def test_create_node_graph():
    g = Graph()
    g.create_node(
        1, "shivam", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    node = g.node("shivam")
    assert node.name == "shivam"
    assert node.properties == {"value": 60, "value_f": 31.3, "value_str": "abc123"}

    with pytest.raises(Exception) as excinfo:
        g.create_node(
            1,
            "shivam",
            properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
        )

    assert "Node already exists" in str(excinfo.value)


def test_create_node_graph_with_deletion():
    g = PersistentGraph()
    g.create_node(
        1, "shivam", properties={"value": 60, "value_f": 31.3, "value_str": "abc123"}
    )
    node = g.node("shivam")
    assert node.name == "shivam"
    assert node.properties == {"value": 60, "value_f": 31.3, "value_str": "abc123"}

    with pytest.raises(Exception) as excinfo:
        g.create_node(
            1,
            "shivam",
            properties={"value": 60, "value_f": 31.3, "value_str": "abc123"},
        )

    assert "Node already exists" in str(excinfo.value)


def test_edge_layer_properties():
    g = Graph()
    g.add_edge(1, "A", "B", properties={"greeting": "howdy"}, layer="layer 1")
    g.add_edge(2, "A", "B", properties={"greeting": "hello"}, layer="layer 2")
    g.add_edge(3, "A", "B", properties={"greeting": "ola"}, layer="layer 2")
    g.add_edge(3, "A", "B", properties={"greeting": "namaste"}, layer="layer 3")

    assert g.edge("A", "B").properties == {"greeting": "namaste"}


def test_add_node_properties_ordered_by_secondary_index():
    g = Graph()
    g.add_node(1, "A", properties={"prop": 1}, secondary_index=3)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=2)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [
        (1, 3),
        (1, 2),
        (1, 1),
    ]


def test_add_node_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.add_node(1, "A", properties={"prop": 1}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.add_node(1, "A", properties={"prop": 1}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=2)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=2)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


def test_create_node_properties_ordered_by_secondary_index():
    g = Graph()
    g.create_node(1, "A", properties={"prop": 1}, secondary_index=3)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=2)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [
        (1, 3),
        (1, 2),
        (1, 1),
    ]


def test_create_node_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.create_node(1, "A", properties={"prop": 1}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.create_node(1, "A", properties={"prop": 1}, secondary_index=1)
    g.add_node(1, "A", properties={"prop": 2}, secondary_index=2)
    g.add_node(1, "A", properties={"prop": 3}, secondary_index=2)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


def test_add_edge_properties_ordered_by_secondary_index():
    g = Graph()
    g.add_edge(1, "A", "B", properties={"prop": 1}, secondary_index=3)
    g.add_edge(1, "A", "B", properties={"prop": 2}, secondary_index=2)
    g.add_edge(1, "A", "B", properties={"prop": 3}, secondary_index=1)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [
        (1, 3),
        (1, 2),
        (1, 1),
    ]


def test_add_edge_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.add_edge(1, "A", "B", properties={"prop": 1}, secondary_index=1)
    g.add_edge(1, "A", "B", properties={"prop": 2}, secondary_index=1)
    g.add_edge(1, "A", "B", properties={"prop": 3}, secondary_index=1)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.add_edge(1, "A", "B", properties={"prop": 1}, secondary_index=1)
    g.add_edge(1, "A", "B", properties={"prop": 2}, secondary_index=2)
    g.add_edge(1, "A", "B", properties={"prop": 3}, secondary_index=2)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


def test_add_properties_properties_ordered_by_secondary_index():
    g = Graph()
    g.add_properties(1, properties={"prop": 1}, secondary_index=3)
    g.add_properties(1, properties={"prop": 2}, secondary_index=2)
    g.add_properties(1, properties={"prop": 3}, secondary_index=1)

    assert g.properties.temporal.get("prop").items() == [(1, 3), (1, 2), (1, 1)]


def test_add_properties_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.add_properties(1, properties={"prop": 1}, secondary_index=1)
    g.add_properties(1, properties={"prop": 2}, secondary_index=1)
    g.add_properties(1, properties={"prop": 3}, secondary_index=1)

    assert g.properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.add_properties(1, properties={"prop": 1}, secondary_index=1)
    g.add_properties(1, properties={"prop": 2}, secondary_index=2)
    g.add_properties(1, properties={"prop": 3}, secondary_index=2)

    assert g.properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


def test_node_add_updates_properties_ordered_by_secondary_index():
    g = Graph()
    g.add_node(1, "A")
    g.node("A").add_updates(1, properties={"prop": 1}, secondary_index=3)
    g.node("A").add_updates(1, properties={"prop": 2}, secondary_index=2)
    g.node("A").add_updates(1, properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [
        (1, 3),
        (1, 2),
        (1, 1),
    ]


def test_node_add_updates_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.add_node(1, "A")
    g.node("A").add_updates(1, properties={"prop": 1}, secondary_index=1)
    g.node("A").add_updates(1, properties={"prop": 2}, secondary_index=1)
    g.node("A").add_updates(1, properties={"prop": 3}, secondary_index=1)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.add_node(1, "A")
    g.node("A").add_updates(1, properties={"prop": 1}, secondary_index=1)
    g.node("A").add_updates(1, properties={"prop": 2}, secondary_index=2)
    g.node("A").add_updates(1, properties={"prop": 3}, secondary_index=2)

    assert g.node("A").properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


def test_edge_add_updates_properties_ordered_by_secondary_index():
    g = Graph()
    g.add_edge(1, "A", "B")
    g.edge("A", "B").add_updates(1, properties={"prop": 1}, secondary_index=3)
    g.edge("A", "B").add_updates(1, properties={"prop": 2}, secondary_index=2)
    g.edge("A", "B").add_updates(1, properties={"prop": 3}, secondary_index=1)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [
        (1, 3),
        (1, 2),
        (1, 1),
    ]


def test_edge_add_updates_properties_overwritten_for_same_secondary_index():
    g = Graph()
    g.add_edge(1, "A", "B")
    g.edge("A", "B").add_updates(1, properties={"prop": 1}, secondary_index=1)
    g.edge("A", "B").add_updates(1, properties={"prop": 2}, secondary_index=1)
    g.edge("A", "B").add_updates(1, properties={"prop": 3}, secondary_index=1)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [(1, 3)]

    g = Graph()
    g.add_edge(1, "A", "B")
    g.edge("A", "B").add_updates(1, properties={"prop": 1}, secondary_index=1)
    g.edge("A", "B").add_updates(1, properties={"prop": 2}, secondary_index=2)
    g.edge("A", "B").add_updates(1, properties={"prop": 3}, secondary_index=2)

    assert g.edge("A", "B").properties.temporal.get("prop").items() == [(1, 1), (1, 3)]


@fixture
def datadir(tmpdir, request):
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)
    if os.path.isdir(test_dir):
        try:
            shutil.copytree(test_dir, str(tmpdir), dirs_exist_ok=True)
            return tmpdir
        except Exception as e:
            raise e
    return tmpdir


# def currently_broken_fuzzy_search(): #TODO: Fix fuzzy searching for properties
# g = Graph()
# g.add_edge(2,"haaroon","hamza", properties={"value":60,"value_f":31.3,"value_str":"abc123"})
# g.add_edge(1,"ben","hamza", properties={"value":59,"value_f":11.4,"value_str":"test test test"})
# g.add_edge(3,"ben","haaroon", properties={"value":199,"value_f":52.6,"value_str":"I gitgit awanna rock right now"})
# g.add_edge(4,"hamza","naomi", properties={"value_str":"I wanna rock right now"})
# assert len(index.fuzzy_search_edges("value_str:\"I wanna nock right now\"",levenshtein_distance=2)) == 2


# def test_search_with_layers(): #TODO: Fix layer seearching
# g = Graph()
# g.add_edge(3,"haaroon","hamza",properties={"value":70,"value_f":11.3,"value_str":"abdsda2c123"},layer="1")
# g.add_edge(4,"ben","naomi",properties={"value":100,"value_f":22.3,"value_str":"ddddd"},layer="2")
# g.add_edge(5,"ben","naomi",properties={"value":100,"value_f":22.3,"value_str":"ddddd"},layer="3")
# index = g.index()

# need to expose actual layer searching
# assert len(index.search_edges("layer:1")) == 1

# assert len(index.search_edges("value_str:ddddd")) == 1
# assert len(index.search_edges("value:>60")) == 2

# l_g = g.layer(["1","3"])
# l_index = l_g.index()
# assert len(index.search_edges("value_str:ddddd")) == 1
# assert len(index.search_edges("value:>60")) == 2
