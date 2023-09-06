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

edges = [(1, 1, 2), (2, 1, 3), (-1, 2, 1), (0, 1, 1), (7, 3, 2), (1, 1, 1)]


def create_graph():
    g = Graph()

    g.add_vertex(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_vertex(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_vertex(6, 3, {"type": "wallet", "cost": 76})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    return g


def create_graph_with_deletions():
    g = GraphWithDeletions()

    g.add_vertex(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_vertex(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_vertex(6, 3, {"type": "wallet", "cost": 76})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})
    g.edge(edges[0][1], edges[0][2]).add_constant_properties({"static": "test"})
    g.delete_edge(10, edges[0][1], edges[0][2])
    return g


def test_graph_len_edge_len():
    g = create_graph()

    assert g.num_vertices() == 3
    assert g.num_edges() == 5


def test_id_iterable():
    g = create_graph()

    assert g.vertices.id().max() == 3
    assert g.vertices.id().min() == 1
    assert set(g.vertices.id().collect()) == {1, 2, 3}
    out_neighbours = g.vertices.out_neighbours().id().collect()
    out_neighbours = (set(n) for n in out_neighbours)
    out_neighbours = dict(zip(g.vertices.id(), out_neighbours))

    assert out_neighbours == {1: {1, 2, 3}, 2: {1}, 3: {2}}


def test_degree_iterable():
    g = create_graph()
    assert g.vertices.degree().min() == 2
    assert g.vertices.degree().max() == 3
    assert g.vertices.in_degree().min() == 1
    assert g.vertices.in_degree().max() == 2
    assert g.vertices.out_degree().min() == 1
    assert g.vertices.out_degree().max() == 3
    assert isclose(g.vertices.degree().mean(), 7 / 3)
    assert g.vertices.degree().sum() == 7
    degrees = g.vertices.degree().collect()
    degrees.sort()
    assert degrees == [2, 2, 3]


def test_vertices_time_iterable():
    g = create_graph()

    assert g.vertices.earliest_time().min() == -1
    assert g.vertices.latest_time().max() == 7


def test_graph_has_edge():
    g = create_graph()

    assert not g.window(-1, 1).has_edge(1, 3)
    assert g.window(-1, 3).has_edge(1, 3)
    assert not g.window(10, 11).has_edge(1, 3)


def test_graph_has_vertex():
    g = create_graph()

    assert g.has_vertex(3)


def test_windowed_graph_has_vertex():
    g = create_graph()

    assert g.window(-1, 1).has_vertex(1)


def test_windowed_graph_get_vertex():
    g = create_graph()

    view = g.window(0, sys.maxsize)

    assert view.vertex(1).id() == 1
    assert view.vertex(10) is None
    assert view.vertex(1).degree() == 3


def test_windowed_graph_degree():
    g = create_graph()

    view = g.window(0, sys.maxsize)

    degrees = [v.degree() for v in view.vertices()]
    degrees.sort()

    assert degrees == [2, 2, 3]

    in_degrees = [v.in_degree() for v in view.vertices()]
    in_degrees.sort()

    assert in_degrees == [1, 1, 2]

    out_degrees = [v.out_degree() for v in view.vertices()]
    out_degrees.sort()

    assert out_degrees == [0, 1, 3]


def test_windowed_graph_get_edge():
    g = create_graph()

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert (view.edge(1, 3).src().id(), view.edge(1, 3).dst().id()) == (1, 3)
    assert view.edge(2, 3) is None
    assert view.edge(6, 5) is None

    assert (view.vertex(1).id(), view.vertex(3).id()) == (1, 3)

    view = g.window(2, 3)
    assert (view.edge(1, 3).src().id(), view.edge(1, 3).dst().id()) == (1, 3)

    view = g.window(3, 7)
    assert view.edge(1, 3) is None


def test_windowed_graph_edges():
    g = create_graph()

    view = g.window(0, sys.maxsize)

    tedges = [v.edges() for v in view.vertices()]
    edges = []
    for e_iter in tedges:
        for e in e_iter:
            edges.append([e.src().id(), e.dst().id()])

    assert edges == [[1, 1], [1, 1], [1, 2], [1, 3], [1, 2], [3, 2], [1, 3], [3, 2]]

    tedges = [v.in_edges() for v in view.vertices()]
    in_edges = []
    for e_iter in tedges:
        for e in e_iter:
            in_edges.append([e.src().id(), e.dst().id()])

    assert in_edges == [[1, 1], [1, 2], [3, 2], [1, 3]]

    tedges = [v.out_edges() for v in view.vertices()]
    out_edges = []
    for e_iter in tedges:
        for e in e_iter:
            out_edges.append([e.src().id(), e.dst().id()])

    assert out_edges == [[1, 1], [1, 2], [1, 3], [3, 2]]


def test_windowed_graph_vertex_ids():
    g = create_graph()

    vs = [v for v in g.window(-1, 2).vertices().id()]
    vs.sort()
    assert vs == [1, 2]  # this makes clear that the end of the range is exclusive

    vs = [v for v in g.window(-5, 3).vertices().id()]
    vs.sort()
    assert vs == [1, 2, 3]


def test_windowed_graph_vertices():
    g = create_graph()

    view = g.window(-1, 0)

    vertices = list(view.vertices().id())

    assert vertices == [1, 2]


def test_windowed_graph_neighbours():
    g = create_graph()

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    neighbours = view.vertices.neighbours().id().collect()
    assert neighbours == [[1, 2, 3], [1, 3], [1, 2]]

    in_neighbours = view.vertices.in_neighbours().id().collect()
    assert in_neighbours == [[1, 2], [1, 3], [1]]

    out_neighbours = view.vertices.out_neighbours().id().collect()
    assert out_neighbours == [[1, 2, 3], [1], [2]]


def test_name():
    g = Graph()
    g.add_vertex(1, "Ben")
    g.add_vertex(1, 10)
    g.add_edge(1, "Ben", "Hamza")
    assert g.vertex(10).name() == "10"
    assert g.vertex("Ben").name() == "Ben"
    assert g.vertex("Hamza").name() == "Hamza"


def test_getitem():
    g = Graph()
    g.add_vertex(0, 1, {"cost": 0})
    g.add_vertex(1, 1, {"cost": 1})

    assert (
        g.vertex(1).properties.temporal.get("cost")
        == g.vertex(1).properties.temporal["cost"]
    )


def test_graph_properties():
    g = create_graph()

    props = {"prop 1": 1, "prop 2": "hi", "prop 3": True}
    g.add_constant_properties(props)

    sp = g.properties.constant.keys()
    sp.sort()
    assert sp == ["prop 1", "prop 2", "prop 3"]
    assert g.properties["prop 1"] == 1

    props = {"prop 4": 11, "prop 5": "world", "prop 6": False}
    g.add_property(1, props)

    props = {"prop 6": True}
    g.add_property(2, props)

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
            assert g.at(time).properties.temporal.get(key) is None
        else:
            assert g.at(time).properties.temporal.get(key).items() == value

    time_history_test(2, "prop 6", [(1, False), (2, True)])
    time_history_test(1, "static prop", None)

    def time_static_property_test(time, key, value):
        assert g.at(time).properties.constant.get(key) == value

    def static_property_test(key, value):
        assert g.properties.constant.get(key) == value

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
    property_test("prop 2", "hi")
    time_static_property_test(2, "prop 3", True)
    time_property_test(2, "prop 3", True)

    # testing properties
    assert g.properties.as_dict() == {
        "prop 1": 1,
        "prop 2": "hi",
        "prop 3": True,
        "prop 4": 11,
        "prop 5": "world",
        "prop 6": True,
    }

    assert g.properties.temporal.latest() == {
        "prop 4": 11,
        "prop 5": "world",
        "prop 6": True,
    }
    assert g.at(2).properties.as_dict() == {
        "prop 1": 1,
        "prop 2": "hi",
        "prop 3": True,
        "prop 4": 11,
        "prop 5": "world",
        "prop 6": True,
    }

    # testing property histories
    assert g.properties.temporal.histories() == {
        "prop 4": [(1, 11)],
        "prop 5": [(1, "world")],
        "prop 6": [(1, False), (2, True)],
    }

    assert g.at(2).properties.temporal.histories() == {
        "prop 4": [(1, 11)],
        "prop 5": [(1, "world")],
        "prop 6": [(1, False), (2, True)],
    }

    # testing property names
    expected_names = sorted(
        ["prop 1", "prop 2", "prop 3", "prop 4", "prop 5", "prop 6"]
    )
    assert sorted(g.properties.keys()) == expected_names

    expected_names_no_static = sorted(["prop 4", "prop 5", "prop 6"])
    assert sorted(g.properties.temporal.keys()) == expected_names_no_static

    assert sorted(g.at(1).properties.temporal.keys()) == expected_names_no_static

    # testing has_property
    assert "prop 4" in g.properties
    assert "prop 7" not in g.properties
    assert "prop 7" not in g.at(1).properties
    assert "prop 1" in g.properties
    assert "prop 2" in g.at(1).properties
    assert "static prop" not in g.properties.constant


def test_vertex_properties():
    g = Graph()
    g.add_edge(1, 1, 1)
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    v = g.add_vertex(1, 1, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    v.add_updates(2, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    v.add_updates(3, props_t3)
    v.add_constant_properties({"static prop": 123})

    # testing property history
    def history_test(key, value):
        if value is None:
            assert g.vertex(1).properties.temporal.get(key) is None
            assert g.vertices.properties.temporal.get(key) is None
            assert g.vertices.out_neighbours().properties.temporal.get(key) is None
        else:
            assert g.vertex(1).properties.temporal.get(key).items() == value
            assert g.vertices.properties.temporal.get(key).items() == [value]
            assert g.vertices.out_neighbours().properties.temporal.get(key).items() == [
                [value]
            ]

    history_test("prop 1", [(1, 1), (2, 2)])
    history_test("prop 2", [(2, 0.6), (3, 0.9)])
    history_test("prop 3", [(1, "hi"), (3, "hello")])
    history_test("prop 4", [(1, True), (2, False), (3, True)])
    history_test("undefined", None)

    def time_history_test(time, key, value):
        if value is None:
            assert g.at(time).vertex(1).properties.temporal.get(key) is None
            assert g.at(time).vertices.properties.temporal.get(key) is None
            assert (
                g.at(time).vertices.out_neighbours().properties.temporal.get(key)
                is None
            )
        else:
            assert g.at(time).vertex(1).properties.temporal.get(key).items() == value
            assert g.at(time).vertices.properties.temporal.get(key).items() == [value]
            assert g.at(time).vertices.out_neighbours().properties.temporal.get(
                key
            ).items() == [[value]]

    time_history_test(1, "prop 4", [(1, True)])
    time_history_test(1, "static prop", None)

    def time_static_property_test(time, key, value):
        gg = g.at(time)
        if value is None:
            assert gg.vertex(1).properties.constant.get(key) is None
            assert gg.vertices.properties.constant.get(key) is None
            assert gg.vertices.out_neighbours().properties.constant.get(key) is None
        else:
            assert gg.vertex(1).properties.constant.get(key) == value
            assert gg.vertices.properties.constant.get(key) == [value]
            assert gg.vertices.out_neighbours().properties.constant.get(key) == [
                [value]
            ]

    def static_property_test(key, value):
        if value is None:
            assert g.vertex(1).properties.constant.get(key) is None
            assert g.vertices.properties.constant.get(key) is None
            assert g.vertices.out_neighbours().properties.constant.get(key) is None
        else:
            assert g.vertex(1).properties.constant.get(key) == value
            assert g.vertices.properties.constant.get(key) == [value]
            assert g.vertices.out_neighbours().properties.constant.get(key) == [[value]]

    time_static_property_test(1, "static prop", 123)
    time_static_property_test(100, "static prop", 123)
    static_property_test("static prop", 123)
    static_property_test("prop 4", None)

    # testing property
    def time_property_test(time, key, value):
        gg = g.at(time)
        if value is None:
            assert gg.vertex(1).properties.get(key) is None
            assert gg.vertices.properties.get(key) is None
            assert gg.vertices.out_neighbours().properties.get(key) is None
        else:
            assert gg.vertex(1).properties.get(key) == value
            assert gg.vertices.properties.get(key) == [value]
            assert gg.vertices.out_neighbours().properties.get(key) == [[value]]

    def property_test(key, value):
        if value is None:
            assert g.vertex(1).properties.get(key) is None
            assert g.vertices.properties.get(key) is None
            assert g.vertices.out_neighbours().properties.get(key) is None
        else:
            assert g.vertex(1).properties.get(key) == value
            assert g.vertices.properties.get(key) == [value]
            assert g.vertices.out_neighbours().properties.get(key) == [[value]]

    def no_static_property_test(key, value):
        if value is None:
            assert g.vertex(1).properties.temporal.get(key) is None
            assert g.vertices.properties.temporal.get(key) is None
            assert g.vertices.out_neighbours().properties.temporal.get(key) is None
        else:
            assert g.vertex(1).properties.temporal.get(key).value() == value
            assert g.vertices.properties.temporal.get(key).value() == [value]
            assert g.vertices.out_neighbours().properties.temporal.get(key).value() == [
                [value]
            ]

    property_test("static prop", 123)
    assert g.vertex(1)["static prop"] == 123
    no_static_property_test("static prop", None)
    no_static_property_test("prop 1", 2)
    time_property_test(2, "prop 2", 0.6)
    time_property_test(1, "prop 2", None)

    # testing properties
    assert g.vertex(1).properties == {
        "prop 2": 0.9,
        "prop 3": "hello",
        "prop 1": 2,
        "prop 4": True,
        "static prop": 123,
    }
    assert g.vertices.properties == {
        "prop 2": [0.9],
        "prop 3": ["hello"],
        "prop 1": [2],
        "prop 4": [True],
        "static prop": [123],
    }
    assert g.vertices.out_neighbours().properties == {
        "prop 2": [[0.9]],
        "prop 3": [["hello"]],
        "prop 1": [[2]],
        "prop 4": [[True]],
        "static prop": [[123]],
    }

    assert g.vertex(1).properties.temporal.latest() == {
        "prop 2": 0.9,
        "prop 3": "hello",
        "prop 1": 2,
        "prop 4": True,
    }
    assert g.vertices.properties.temporal.latest() == {
        "prop 2": [0.9],
        "prop 3": ["hello"],
        "prop 1": [2],
        "prop 4": [True],
    }
    assert g.vertices.out_neighbours().properties.temporal.latest() == {
        "prop 2": [[0.9]],
        "prop 3": [["hello"]],
        "prop 1": [[2]],
        "prop 4": [[True]],
    }

    assert g.at(2).vertex(1).properties == {
        "prop 1": 2,
        "prop 4": False,
        "prop 2": 0.6,
        "static prop": 123,
        "prop 3": "hi",
    }
    assert g.at(2).vertices.properties == {
        "prop 1": [2],
        "prop 4": [False],
        "prop 2": [0.6],
        "static prop": [123],
        "prop 3": ["hi"],
    }
    assert g.at(2).vertices.out_neighbours().properties == {
        "prop 1": [[2]],
        "prop 4": [[False]],
        "prop 2": [[0.6]],
        "static prop": [[123]],
        "prop 3": [["hi"]],
    }

    # testing property histories
    assert g.vertex(1).properties.temporal == {
        "prop 3": [(1, "hi"), (3, "hello")],
        "prop 1": [(1, 1), (2, 2)],
        "prop 4": [(1, True), (2, False), (3, True)],
        "prop 2": [(2, 0.6), (3, 0.9)],
    }
    assert g.vertices.properties.temporal == {
        "prop 3": [[(1, "hi"), (3, "hello")]],
        "prop 1": [[(1, 1), (2, 2)]],
        "prop 4": [[(1, True), (2, False), (3, True)]],
        "prop 2": [[(2, 0.6), (3, 0.9)]],
    }
    assert g.vertices.out_neighbours().properties.temporal == {
        "prop 3": [[[(1, "hi"), (3, "hello")]]],
        "prop 1": [[[(1, 1), (2, 2)]]],
        "prop 4": [[[(1, True), (2, False), (3, True)]]],
        "prop 2": [[[(2, 0.6), (3, 0.9)]]],
    }

    assert g.at(2).vertex(1).properties.temporal == {
        "prop 2": [(2, 0.6)],
        "prop 4": [(1, True), (2, False)],
        "prop 1": [(1, 1), (2, 2)],
        "prop 3": [(1, "hi")],
    }
    assert g.at(2).vertices.properties.temporal == {
        "prop 2": [[(2, 0.6)]],
        "prop 4": [[(1, True), (2, False)]],
        "prop 1": [[(1, 1), (2, 2)]],
        "prop 3": [[(1, "hi")]],
    }
    assert g.at(2).vertices.out_neighbours().properties.temporal == {
        "prop 2": [[[(2, 0.6)]]],
        "prop 4": [[[(1, True), (2, False)]]],
        "prop 1": [[[(1, 1), (2, 2)]]],
        "prop 3": [[[(1, "hi")]]],
    }

    # testing property names
    expected_names = sorted(["prop 4", "prop 1", "prop 2", "prop 3", "static prop"])
    assert sorted(g.vertex(1).properties.keys()) == expected_names
    assert sorted(g.vertices.properties.keys()) == expected_names
    assert sorted(g.vertices.out_neighbours().properties.keys()) == expected_names

    expected_names_no_static = sorted(["prop 4", "prop 1", "prop 2", "prop 3"])
    assert sorted(g.vertex(1).properties.temporal.keys()) == expected_names_no_static
    assert sorted(g.vertices.properties.temporal.keys()) == expected_names_no_static
    assert (
        sorted(g.vertices.out_neighbours().properties.temporal.keys())
        == expected_names_no_static
    )

    expected_names_no_static_at_1 = sorted(["prop 4", "prop 1", "prop 3"])
    assert (
        sorted(g.at(1).vertex(1).properties.temporal.keys())
        == expected_names_no_static_at_1
    )
    assert (
        sorted(g.at(1).vertices.properties.temporal.keys())
        == expected_names_no_static_at_1
    )
    assert (
        sorted(g.at(1).vertices.out_neighbours().properties.temporal.keys())
        == expected_names_no_static_at_1
    )

    # testing has_property
    assert "prop 4" in g.vertex(1).properties
    assert "prop 4" in g.vertices.properties
    assert "prop 4" in g.vertices.out_neighbours().properties

    assert "prop 2" in g.vertex(1).properties
    assert "prop 2" in g.vertices.properties
    assert "prop 2" in g.vertices.out_neighbours().properties

    assert "prop 5" not in g.vertex(1).properties
    assert "prop 5" not in g.vertices.properties
    assert "prop 5" not in g.vertices.out_neighbours().properties

    assert "prop 2" not in g.at(1).vertex(1).properties
    assert "prop 2" not in g.at(1).vertices.properties
    assert "prop 2" not in g.at(1).vertices.out_neighbours().properties

    assert "static prop" in g.vertex(1).properties
    assert "static prop" in g.vertices.properties
    assert "static prop" in g.vertices.out_neighbours().properties

    assert "static prop" in g.at(1).vertex(1).properties
    assert "static prop" in g.at(1).vertices.properties
    assert "static prop" in g.at(1).vertices.out_neighbours().properties

    assert "static prop" not in g.at(1).vertex(1).properties.temporal
    assert "static prop" not in g.at(1).vertices.properties.temporal
    assert "static prop" not in g.at(1).vertices.out_neighbours().properties.temporal

    assert "static prop" in g.vertex(1).properties.constant
    assert "static prop" in g.vertices.properties.constant
    assert "static prop" in g.vertices.out_neighbours().properties.constant

    assert "prop 2" not in g.vertex(1).properties.constant
    assert "prop 2" not in g.vertices.properties.constant
    assert "prop 2" not in g.vertices.out_neighbours().properties.constant

    assert "static prop" in g.at(1).vertex(1).properties.constant
    assert "static prop" in g.at(1).vertices.properties.constant
    assert "static prop" in g.at(1).vertices.out_neighbours().properties.constant


def test_edge_properties():
    g = Graph()
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    e = g.add_edge(1, 1, 2, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    e.add_updates(2, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    e.add_updates(3, props_t3)

    e.add_constant_properties({"static prop": 123})

    # testing property history
    assert g.edge(1, 2).properties.temporal.get("prop 1") == [(1, 1), (2, 2)]
    assert g.edge(1, 2).properties.temporal.get("prop 2") == [(2, 0.6), (3, 0.9)]
    assert g.edge(1, 2).properties.temporal.get("prop 3") == [(1, "hi"), (3, "hello")]
    assert g.edge(1, 2).properties.temporal.get("prop 4") == [
        (1, True),
        (2, False),
        (3, True),
    ]
    assert g.edge(1, 2).properties.temporal.get("undefined") is None
    assert g.at(1).edge(1, 2).properties.temporal.get("prop 4") == [(1, True)]
    assert g.at(1).edge(1, 2).properties.temporal.get("static prop") is None

    assert g.at(1).edge(1, 2).properties.constant.get("static prop") == 123
    assert g.at(100).edge(1, 2).properties.constant.get("static prop") == 123
    assert g.edge(1, 2).properties.constant.get("static prop") == 123
    assert g.edge(1, 2).properties.constant.get("prop 4") is None

    # testing property
    assert g.edge(1, 2).properties.get("static prop") == 123
    assert g.edge(1, 2)["static prop"] == 123
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
        "static prop": 123,
    }

    assert g.edge(1, 2).properties.temporal.latest() == {
        "prop 2": 0.9,
        "prop 3": "hello",
        "prop 1": 2,
        "prop 4": True,
    }

    assert g.at(2).edge(1, 2).properties == {
        "prop 1": 2,
        "prop 4": False,
        "prop 2": 0.6,
        "static prop": 123,
        "prop 3": "hi",
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
        "prop 4": [(1, True), (2, False)],
        "prop 1": [(1, 1), (2, 2)],
        "prop 3": [(1, "hi")],
    }

    # testing property names
    assert sorted(g.edge(1, 2).properties.keys()) == sorted(
        ["prop 4", "prop 1", "prop 2", "prop 3", "static prop"]
    )

    assert sorted(g.edge(1, 2).properties.temporal.keys()) == sorted(
        ["prop 4", "prop 1", "prop 2", "prop 3"]
    )

    assert sorted(g.at(1).edge(1, 2).properties.temporal.keys()) == sorted(
        ["prop 4", "prop 1", "prop 3"]
    )

    # testing has_property
    assert "prop 4" in g.edge(1, 2).properties
    assert "prop 2" in g.edge(1, 2).properties
    assert "prop 5" not in g.edge(1, 2).properties
    assert "prop 2" not in g.at(1).edge(1, 2).properties
    assert "static prop" in g.edge(1, 2).properties
    assert "static prop" in g.at(1).edge(1, 2).properties
    assert "static prop" not in g.at(1).edge(1, 2).properties.temporal

    assert "static prop" in g.edge(1, 2).properties.constant
    assert "prop 2" not in g.edge(1, 2).properties.constant
    assert "static prop" in g.at(1).edge(1, 2).properties.constant


def test_graph_as_property():
    g = Graph()
    g.add_edge(0, 1, 2, {"graph": g})
    assert "graph" in g.edge(1, 2).properties
    assert g.edge(1, 2).properties["graph"].has_edge(1, 2)


def test_map_and_list_property():
    g = Graph()
    g.add_edge(0, 1, 2, {"map": {"test": 1, "list": [1, 2, 3]}})
    e_props = g.edge(1, 2).properties
    assert "map" in e_props
    assert e_props["map"]["test"] == 1
    assert e_props["map"]["list"] == [1, 2, 3]


def test_exploded_edge_time():
    g = graph_loader.lotr_graph()
    e = g.edge("Frodo", "Gandalf")
    his = e.history()
    exploded_his = []
    for ee in e.explode():
        exploded_his.append(ee.time())
    assert his == exploded_his


def test_algorithms():
    g = Graph()
    lotr_graph = graph_loader.lotr_graph()
    g.add_edge(1, 1, 2, {"prop1": 1})
    g.add_edge(2, 2, 3, {"prop1": 1})
    g.add_edge(3, 3, 1, {"prop1": 1})

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
    assert lotr_clustering_coefficient == 0.1984313726425171
    assert lotr_local_triangle_count == 253


def test_graph_time_api():
    g = create_graph()

    earliest_time = g.earliest_time()
    latest_time = g.latest_time()
    assert len(list(g.rolling(1))) == latest_time - earliest_time + 1
    assert len(list(g.expanding(2))) == math.ceil((latest_time + 1 - earliest_time) / 2)

    w = g.window(2, 6)
    assert len(list(w.rolling(window=10, step=3))) == 2


def test_save_load_graph():
    g = create_graph()
    g.add_vertex(1, 11, {"type": "wallet", "balance": 99.5})
    g.add_vertex(2, 12, {"type": "wallet", "balance": 10.0})
    g.add_vertex(3, 13, {"type": "wallet", "balance": 76})
    g.add_edge(4, 11, 12, {"prop1": 1, "prop2": 9.8, "prop3": "test"})
    g.add_edge(5, 12, 13, {"prop1": 1321, "prop2": 9.8, "prop3": "test"})
    g.add_edge(6, 13, 11, {"prop1": 645, "prop2": 9.8, "prop3": "test"})

    tmpdirname = tempfile.TemporaryDirectory()
    graph_path = tmpdirname.name + "/test_graph.bin"
    g.save_to_file(graph_path)

    del g

    g = Graph.load_from_file(graph_path)

    view = g.window(0, 10)
    assert g.has_vertex(13)
    assert view.vertex(13).in_degree() == 1
    assert view.vertex(13).out_degree() == 1
    assert view.vertex(13).degree() == 2

    triangles = algorithms.local_triangle_count(
        view, 13
    )  # How many triangles is 13 involved in
    assert triangles == 1

    v = view.vertex(11)
    assert v.properties.temporal == {"type": [(1, "wallet")], "balance": [(1, 99.5)]}

    tmpdirname.cleanup()


def test_graph_at():
    g = create_graph()

    view = g.at(2)
    assert view.vertex(1).degree() == 3
    assert view.vertex(3).degree() == 1

    view = g.at(7)
    assert view.vertex(3).degree() == 2


def test_add_node_string():
    g = Graph()

    g.add_vertex(0, 1, {})
    g.add_vertex(1, "haaroon", {})
    g.add_vertex(1, "haaroon", {})  # add same vertex twice used to cause an exception

    assert g.has_vertex(1)
    assert g.has_vertex("haaroon")


def test_add_edge_string():
    g = Graph()

    g.add_edge(0, 1, 2, {})
    g.add_edge(1, "haaroon", "ben", {})

    assert g.has_vertex(1)
    assert g.has_vertex(2)
    assert g.has_vertex("haaroon")
    assert g.has_vertex("ben")

    assert g.has_edge(1, 2)
    assert g.has_edge("haaroon", "ben")


def test_all_neighbours_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(4, 2, 4, {})

    view = g.at(2)
    v = view.vertex(2)
    assert list(v.window(0, 2).in_neighbours().id()) == [1]
    assert list(v.window(0, 2).out_neighbours().id()) == [3]
    assert list(v.window(0, 2).neighbours().id()) == [1, 3]


def test_all_degrees_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(3, 4, 2, {})
    g.add_edge(4, 2, 4, {})
    g.add_edge(5, 2, 1, {})

    view = g.at(4)
    v = view.vertex(2)
    assert v.window(0, 4).in_degree() == 3
    assert v.window(t_start=2).in_degree() == 2
    assert v.window(t_end=3).in_degree() == 2
    assert v.window(0, 4).out_degree() == 1
    assert v.window(t_start=2).out_degree() == 1
    assert v.window(t_end=3).out_degree() == 1
    assert v.window(0, 4).degree() == 3
    assert v.window(t_start=2).degree() == 2
    assert v.window(t_end=3).degree() == 2


def test_all_edge_window():
    g = Graph()
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(3, 4, 2, {})
    g.add_edge(4, 2, 4, {})
    g.add_edge(5, 2, 1, {})

    view = g.at(4)
    v = view.vertex(2)
    assert sorted(v.window(0, 4).in_edges().src().id()) == [1, 3, 4]
    assert sorted(v.window(t_end=4).in_edges().src().id()) == [1, 3, 4]
    assert sorted(v.window(t_start=2).in_edges().src().id()) == [3, 4]
    assert sorted(v.window(0, 4).out_edges().dst().id()) == [3]
    assert sorted(v.window(t_end=3).out_edges().dst().id()) == [3]
    assert sorted(v.window(t_start=2).out_edges().dst().id()) == [4]
    assert sorted((e.src().id(), e.dst().id()) for e in v.window(0, 4).edges()) == [
        (1, 2),
        (2, 3),
        (3, 2),
        (4, 2),
    ]
    assert sorted((e.src().id(), e.dst().id()) for e in v.window(t_end=4).edges()) == [
        (1, 2),
        (2, 3),
        (3, 2),
        (4, 2),
    ]
    assert sorted(
        (e.src().id(), e.dst().id()) for e in v.window(t_start=1).edges()
    ) == [(1, 2), (2, 3), (2, 4), (3, 2), (4, 2)]


def test_static_prop_change():
    # with pytest.raises(Exception):
    g = Graph()
    v = g.add_vertex(0, 1)
    v.add_constant_properties({"name": "value1"})

    expected_msg = (
        """Exception: Failed to mutate graph\n"""
        """Caused by:\n"""
        """  -> cannot change property for vertex '1'\n"""
        """  -> cannot mutate static property 'name'\n"""
        """  -> cannot set previous value 'Some(Str("value1"))' to 'Some(Str("value2"))' in position '0'"""
    )

    # with pytest.raises(Exception, match=re.escape(expected_msg)):
    with pytest.raises(Exception):
        v.add_constant_properties({"name": "value2"})


def test_triplet_count():
    g = Graph()

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})

    v = g.at(1)
    assert algorithms.triplet_count(v) == 3


def test_global_clustering_coeffficient():
    g = Graph()

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})
    g.add_edge(0, 4, 2, {})
    g.add_edge(0, 4, 1, {})
    g.add_edge(0, 5, 2, {})

    v = g.at(1)
    assert algorithms.global_clustering_coefficient(v) == 0.5454545454545454


def test_edge_time_apis():
    g = Graph()

    g.add_edge(1, 1, 2, {"prop2": 10})
    g.add_edge(2, 2, 4, {"prop2": 11})
    g.add_edge(3, 4, 5, {"prop2": 12})
    g.add_edge(4, 1, 5, {"prop2": 13})

    v = g.vertex(1)
    e = g.edge(1, 2)

    for e in e.expanding(1):
        assert e.src().name() == "1"
        assert e.dst().name() == "2"

    ls = []
    for e in v.edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ["1", "2", "1", "5"]

    v = g.vertex(2)
    ls = []
    for e in v.in_edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ["1", "2"]

    ls = []
    for e in v.out_edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ["2", "4"]


def test_edge_earliest_latest_time():
    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(1, 1, 3, {})
    g.add_edge(2, 1, 3, {})

    assert g.edge(1, 2).earliest_time() == 0
    assert g.edge(1, 2).latest_time() == 2

    assert list(g.vertex(1).edges().earliest_time()) == [0, 0]
    assert list(g.vertex(1).edges().latest_time()) == [2, 2]
    assert list(g.vertex(1).at(1).edges().earliest_time()) == [0, 0]
    assert list(g.vertex(1).at(1).edges().latest_time()) == [1, 1]


def test_vertex_earliest_time():
    g = Graph()
    g.add_vertex(0, 1, {})
    g.add_vertex(1, 1, {})
    g.add_vertex(2, 1, {})

    view = g.at(1)
    assert view.vertex(1).earliest_time() == 0
    assert view.vertex(1).latest_time() == 1
    view = g.at(3)
    assert view.vertex(1).earliest_time() == 0
    assert view.vertex(1).latest_time() == 2


def test_vertex_history():
    g = Graph()

    g.add_vertex(1, 1, {})
    g.add_vertex(2, 1, {})
    g.add_vertex(3, 1, {})
    g.add_vertex(4, 1, {})
    g.add_vertex(8, 1, {})

    g.add_vertex(4, "Lord Farquaad", {})
    g.add_vertex(6, "Lord Farquaad", {})
    g.add_vertex(7, "Lord Farquaad", {})
    g.add_vertex(8, "Lord Farquaad", {})

    assert g.vertex(1).history() == [1, 2, 3, 4, 8]
    assert g.vertex("Lord Farquaad").history() == [4, 6, 7, 8]

    view = g.window(1, 8)

    assert view.vertex(1).history() == [1, 2, 3, 4]
    assert view.vertex("Lord Farquaad").history() == [4, 6, 7]


def test_edge_history():
    g = Graph()

    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 2)
    g.add_edge(4, 1, 4)

    view = g.window(1, 5)

    assert g.edge(1, 2).history() == [1, 3]
    assert view.edge(1, 4).history() == [4]


def test_lotr_edge_history():
    g = graph_loader.lotr_graph()

    assert g.edge("Frodo", "Gandalf").history() == [
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
    ]
    assert g.at(1000).edge("Frodo", "Gandalf").history() == [329, 555, 861]
    assert g.edge("Frodo", "Gandalf").at(1000).history() == [329, 555, 861]
    assert g.window(100, 1000).edge("Frodo", "Gandalf").history() == [329, 555, 861]
    assert g.edge("Frodo", "Gandalf").window(100, 1000).history() == [329, 555, 861]


def gen_graph():
    g = Graph()
    g.add_edge(10, 1, 3, {})
    g.add_edge(11, 1, 2, {})
    g.add_edge(12, 1, 2, {})
    g.add_edge(9, 1, 2, {})
    g.add_edge(12, 2, 4, {})
    g.add_edge(13, 2, 5, {})
    g.add_edge(14, 5, 5, {})
    g.add_edge(14, 5, 4, {})
    g.add_edge(5, 4, 6, {})
    g.add_edge(15, 4, 7, {})
    g.add_edge(10, 4, 7, {})
    g.add_edge(10, 5, 8, {})
    return g


def test_connected_components():
    g = gen_graph()
    actual = algorithms.weakly_connected_components(g, 20)
    expected = {"1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1, "7": 1, "8": 1}
    assert actual.get_all() == expected
    assert actual.get("1") == 1


def test_algo_result():
    g = gen_graph()

    actual = algorithms.weakly_connected_components(g, 20)
    expected = {"1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1, "7": 1, "8": 1}
    assert actual.get_all() == expected
    assert actual.get("1") == 1
    assert actual.get("not a node") == None
    expected_array = [
        ("1", 1),
        ("2", 1),
        ("3", 1),
        ("4", 1),
        ("5", 1),
        ("6", 1),
        ("7", 1),
        ("8", 1),
    ]
    assert sorted(actual.sort_by_value()) == expected_array
    assert actual.sort_by_key() == sorted(expected_array, reverse=True)
    assert actual.sort_by_key(reverse=False) == expected_array
    assert sorted(actual.top_k(8)) == expected_array
    assert len(actual.group_by()[1]) == 8
    assert type(actual.to_df()) == pandas.core.frame.DataFrame
    df = actual.to_df()
    expected_result = pd.DataFrame({"Key": ["1"], "Value": [1]})
    row_with_one = df[df["Key"] == "1"]
    row_with_one.reset_index(inplace=True, drop=True)
    assert row_with_one.equals(expected_result)
    # Algo Str u64
    actual = algorithms.weakly_connected_components(g)
    all_res = actual.get_all()
    sorted_res = {k: all_res[k] for k in sorted(all_res)}
    assert sorted_res == {
        "1": 1,
        "2": 1,
        "3": 1,
        "4": 1,
        "5": 1,
        "6": 1,
        "7": 1,
        "8": 1,
    }
    # algo str f64
    actual = algorithms.pagerank(g)
    expected_result = {
        "3": 0.10274080842110422,
        "2": 0.10274080842110422,
        "4": 0.1615298183542792,
        "6": 0.14074777909144864,
        "1": 0.07209850165402759,
        "5": 0.1615298183542792,
        "7": 0.14074777909144864,
        "8": 0.11786468661230831,
    }
    assert actual.get_all() == expected_result
    assert actual.get("Not a node") == None
    assert len(actual.to_df()) == 8
    # algo str vector
    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    assert sorted(actual.get_all()) == ["1", "2", "3", "4", "5", "6", "7", "8"]


def test_page_rank():
    g = gen_graph()
    actual = algorithms.pagerank(g)
    expected = {
        "1": 0.07209850165402759,
        "2": 0.10274080842110422,
        "3": 0.10274080842110422,
        "4": 0.1615298183542792,
        "5": 0.1615298183542792,
        "6": 0.14074777909144864,
        "7": 0.14074777909144864,
        "8": 0.11786468661230831,
    }
    assert actual.get_all() == expected


def test_temporal_reachability():
    g = gen_graph()

    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    expected = {
        "1": [(11, "start")],
        "2": [(11, "start"), (12, "1"), (11, "1")],
        "3": [],
        "4": [(12, "2")],
        "5": [(13, "2")],
        "6": [],
        "7": [],
        "8": [],
    }

    assert actual.get_all() == expected


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
    g.add_edge(0, 1, 4, layer="layer2")

    assert g.default_layer().num_edges() == 1
    assert g.layers(["layer1"]).num_edges() == 1
    assert g.layers(["layer2"]).num_edges() == 1


def test_layer_vertex():
    g = Graph()

    g.add_edge(0, 1, 2, layer="layer1")
    g.add_edge(0, 2, 3, layer="layer2")
    g.add_edge(3, 2, 4, layer="layer1")
    neighbours = g.layers(["layer1", "layer2"]).vertex(1).neighbours().collect()
    assert sorted(neighbours[0].layers(["layer2"]).edges().id()) == [(2, 3)]
    assert sorted(g.layers(["layer2"]).vertex(neighbours[0].name()).edges().id()) == [
        (2, 3)
    ]
    assert sorted(g.layers(["layer1"]).vertex(neighbours[0].name()).edges().id()) == [
        (1, 2),
        (2, 4),
    ]
    assert sorted(g.layers(["layer1"]).edges().id()) == [(1, 2), (2, 4)]
    assert sorted(g.layers(["layer1", "layer2"]).edges().id()) == [
        (1, 2),
        (2, 3),
        (2, 4),
    ]


def test_rolling_as_iterable():
    g = Graph()

    g.add_vertex(1, 1)
    g.add_vertex(4, 4)

    rolling = g.rolling(1)

    # a normal operation is reusing the object returned by rolling twice, to get both results and an index.
    # So the following should work fine:
    n_vertices = [w.num_vertices() for w in rolling]
    time_index = [w.start() for w in rolling]

    assert n_vertices == [1, 0, 0, 1]
    assert time_index == [1, 2, 3, 4]


def test_layer_name():
    g = Graph()

    g.add_edge(0, 0, 1)
    g.add_edge(0, 0, 2, layer="awesome layer")

    assert g.edge(0, 1).layer_names() == ["_default"]
    assert g.edge(0, 2).layer_names() == ["awesome layer"]


def test_window_size():
    g = Graph()
    g.add_vertex(1, 1)
    g.add_vertex(4, 4)

    assert g.window_size() == 4


def test_time_index():
    g = Graph()

    w = g.window("2020-01-01", "2020-01-03")
    rolling = w.rolling("1 day")
    time_index = rolling.time_index()
    assert list(time_index) == [
        datetime.datetime(2020, 1, 1, 23, 59, 59, 999000),
        datetime.datetime(2020, 1, 2, 23, 59, 59, 999000),
    ]

    w = g.window(1, 3)
    rolling = w.rolling(1)
    time_index = rolling.time_index()
    assert list(time_index) == [1, 2]

    w = g.window(0, 100)
    rolling = w.rolling(50)
    time_index = rolling.time_index(center=True)
    assert list(time_index) == [25, 75]


def test_datetime_props():
    g = Graph()
    dt1 = datetime.datetime(2020, 1, 1, 23, 59, 59, 999000)
    g.add_vertex(0, 0, {"time": dt1})
    assert g.vertex(0).properties.get("time") == dt1

    dt2 = datetime.datetime(2020, 1, 1, 23, 59, 59, 999999)
    g.add_vertex(0, 1, {"time": dt2})
    assert g.vertex(1).properties.get("time") == dt2


def test_date_time():
    g = Graph()

    g.add_edge("2014-02-02", 1, 2)
    g.add_edge("2014-02-03", 1, 3)
    g.add_edge("2014-02-04", 1, 4)
    g.add_edge("2014-02-05", 1, 2)

    assert g.earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert g.latest_date_time() == datetime.datetime(2014, 2, 5, 0, 0)

    e = g.edge(1, 3)
    exploded_edges = []
    for edge in e.explode():
        exploded_edges.append(edge.date_time())
    assert exploded_edges == [datetime.datetime(2014, 2, 3)]
    assert g.edge(1, 2).earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert g.edge(1, 2).latest_date_time() == datetime.datetime(2014, 2, 5, 0, 0)

    assert g.vertex(1).earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert g.vertex(1).latest_date_time() == datetime.datetime(2014, 2, 5, 0, 0)


def test_date_time_window():
    g = Graph()

    g.add_edge("2014-02-02", 1, 2)
    g.add_edge("2014-02-03", 1, 3)
    g.add_edge("2014-02-04", 1, 4)
    g.add_edge("2014-02-05", 1, 2)
    g.add_edge("2014-02-06", 1, 2)

    view = g.window("2014-02-02", "2014-02-04")
    view2 = g.window("2014-02-02", "2014-02-05")

    assert view.start_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view.end_date_time() == datetime.datetime(2014, 2, 4, 0, 0)

    assert view.earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view.latest_date_time() == datetime.datetime(2014, 2, 3, 0, 0)

    assert view2.edge(1, 2).start_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view2.edge(1, 2).end_date_time() == datetime.datetime(2014, 2, 5, 0, 0)

    assert view.vertex(1).earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view.vertex(1).latest_date_time() == datetime.datetime(2014, 2, 3, 0, 0)

    e = view.edge(1, 2)
    exploded_edges = []
    for edge in e.explode():
        exploded_edges.append(edge.date_time())
    assert exploded_edges == [datetime.datetime(2014, 2, 2)]


def test_datetime_add_vertex():
    g = Graph()
    g.add_vertex(datetime.datetime(2014, 2, 2), 1)
    g.add_vertex(datetime.datetime(2014, 2, 3), 2)
    g.add_vertex(datetime.datetime(2014, 2, 4), 2)
    g.add_vertex(datetime.datetime(2014, 2, 5), 4)
    g.add_vertex(datetime.datetime(2014, 2, 6), 5)

    view = g.window("2014-02-02", "2014-02-04")
    view2 = g.window("2014-02-02", "2014-02-05")

    assert view.start_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view.end_date_time() == datetime.datetime(2014, 2, 4, 0, 0)

    assert view2.earliest_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view2.latest_date_time() == datetime.datetime(2014, 2, 4, 0, 0)

    assert view2.vertex(1).start_date_time() == datetime.datetime(2014, 2, 2, 0, 0)
    assert view2.vertex(1).end_date_time() == datetime.datetime(2014, 2, 5, 0, 0)

    assert view.vertex(2).earliest_date_time() == datetime.datetime(2014, 2, 3, 0, 0)
    assert view.vertex(2).latest_date_time() == datetime.datetime(2014, 2, 3, 0, 0)


def test_equivalent_vertices_edges_and_sets():
    g = Graph()
    g.add_vertex(1, 1)
    g.add_vertex(1, 2)
    g.add_vertex(1, 3)

    g.add_edge(1, 1, 2)
    g.add_edge(1, 2, 3)

    assert g.vertex(1) == g.vertex(1)
    assert list(g.vertex(1).neighbours())[0] == list(g.vertex(3).neighbours())[0]
    assert set(g.vertex(1).neighbours()) == set(g.vertex(3).neighbours())
    assert set(g.vertex(1).out_edges()) == set(g.vertex(2).in_edges())

    assert g.edge(1, 1) == g.edge(1, 1)


def test_subgraph():
    g = create_graph()
    empty_graph = g.subgraph([])
    assert empty_graph.vertices.collect() == []

    vertex1 = g.vertices[1]
    subgraph = g.subgraph([vertex1])
    assert subgraph.vertices.collect() == [vertex1]

    subgraph_from_str = g.subgraph(["1"])
    assert subgraph_from_str.vertices.collect() == [vertex1]

    subgraph_from_int = g.subgraph([1])
    assert subgraph_from_int.vertices.collect() == [vertex1]

    mg = subgraph.materialize()
    assert mg.vertices.collect()[0].properties["type"] == "wallet"
    assert mg.vertices.collect()[0].name() == "1"

    props = {"prop 4": 11, "prop 5": "world", "prop 6": False}
    mg.add_property(1, props)

    props = {"prop 1": 1, "prop 2": "hi", "prop 3": True}
    mg.add_constant_properties(props)
    x = mg.properties.keys()
    x.sort()
    assert x == ["prop 1", "prop 2", "prop 3", "prop 4", "prop 5", "prop 6"]


def test_materialize_graph():
    g = Graph()

    edges = [(1, 1, 2), (2, 1, 3), (-1, 2, 1), (0, 1, 1), (7, 3, 2), (1, 1, 1)]

    g.add_vertex(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_vertex(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_vertex(6, 3, {"type": "wallet", "cost": 76})
    g.add_vertex(6, 4).add_constant_properties({"abc": "xyz"})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1, "prop2": 9.8, "prop3": "test"})

    g.add_edge(8, 2, 4)

    sprop = {"sprop 1": "kaggle", "sprop 2": True}
    g.add_constant_properties(sprop)
    assert g.properties.constant == sprop

    mg = g.materialize()

    assert mg.vertex(1).properties.get("type") == "wallet"
    assert mg.vertex(4).properties == {"abc": "xyz"}
    assert mg.vertex(4).properties.constant.get("abc") == "xyz"
    assert mg.vertex(1).history() == [-1, 0, 1, 2]
    assert mg.vertex(4).history() == [6, 8]
    assert mg.vertices().id().collect() == [1, 2, 3, 4]
    assert set(mg.edges().id()) == {(1, 1), (1, 2), (1, 3), (2, 1), (3, 2), (2, 4)}
    assert g.vertices.id().collect() == mg.vertices.id().collect()
    assert set(g.edges().id()) == set(mg.edges().id())
    assert mg.vertex(1).properties.constant == {}
    assert mg.vertex(4).properties.constant == {"abc": "xyz"}
    assert g.edge(1, 2).id() == (1, 2)
    assert mg.edge(1, 2).id() == (1, 2)
    assert mg.has_edge(1, 2)
    assert g.has_edge(1, 2)
    assert mg.has_edge(2, 1)
    assert g.has_edge(2, 1)

    sprop2 = {"sprop 3": 11, "sprop 4": 10}
    mg.add_constant_properties(sprop2)
    sprop.update(sprop2)
    assert mg.properties.constant == sprop


def test_deletions():
    g = create_graph_with_deletions()
    for e in edges:
        assert g.at(e[0]).has_edge(e[1], e[2])

    assert not g.window(start=11).has_edge(edges[0][1], edges[0][2])
    for e in edges[1:]:
        assert g.window(start=11).has_edge(e[1], e[2])

    assert list(g.edge(edges[0][1], edges[0][2]).explode().latest_time()) == [10]


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

    assert g.vertices().id().collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges():
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src().id(), e.dst().id(), weight, marbles))

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

    assert g.vertices().id().collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges():
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src().id(), e.dst().id(), weight, marbles))

    assert edges == [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    vertices = []
    for v in g.vertices():
        name = v["name"]
        vertices.append((v.id(), name))

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
        src="src",
        dst="dst",
        time="time",
        props=["weight", "marbles"],
        vertex_df=vertices_df,
        vertex_col="id",
        vertex_time_col="time",
        vertex_props=["name"],
    )

    assert g.vertices().id().collect() == [1, 2, 3, 4, 5, 6]
    edges = []
    for e in g.edges():
        weight = e["weight"]
        marbles = e["marbles"]
        edges.append((e.src().id(), e.dst().id(), weight, marbles))

    assert edges == [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    vertices = []
    for v in g.vertices():
        name = v["name"]
        vertices.append((v.id(), name))

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
    assert g.vertices().properties.constant.get("type").collect() == [
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
    ]
    assert g.vertices().properties.constant.get("tag").collect() == [
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
    assert g.vertices().properties.constant.get("type").collect() == [
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
    )

    assert g.layers(["test_layer"]).edges().src().id().collect() == [1, 2, 3, 4, 5]
    assert g.edges().properties.constant.get("type").collect() == [
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
        {"test_layer": "Edge"},
    ]
    assert g.edges().properties.constant.get("tag").collect() == [
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
        {"test_layer": "test_tag"},
    ]
    assert g.edges().properties.constant.get("marbles_const").collect() == [
        {"test_layer": "red"},
        {"test_layer": "blue"},
        {"test_layer": "green"},
        {"test_layer": "yellow"},
        {"test_layer": "purple"},
    ]

    g = Graph()
    g.load_edges_from_pandas(
        edges_df, "src", "dst", "time", ["weight", "marbles"], layer_in_df="layers"
    )
    assert g.layers(["layer 1"]).edges().src().id().collect() == [1]
    assert g.layers(["layer 1", "layer 2"]).edges().src().id().collect() == [1, 2]
    assert g.layers(["layer 1", "layer 2", "layer 3"]).edges().src().id().collect() == [
        1,
        2,
        3,
    ]
    assert g.layers(["layer 1", "layer 4", "layer 5"]).edges().src().id().collect() == [
        1,
        4,
        5,
    ]

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        layer="test_layer",
        vertex_df=vertices_df,
        vertex_col="id",
        vertex_time_col="time",
        vertex_props=["name"],
        vertex_shared_const_props={"type": "Person"},
    )
    assert g.vertices().properties.constant.get("type").collect() == [
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
        "Person",
    ]
    assert g.layers(["test_layer"]).edges().src().id().collect() == [1, 2, 3, 4, 5]

    g = Graph.load_from_pandas(
        edges_df,
        "src",
        "dst",
        "time",
        layer_in_df="layers",
        vertex_df=vertices_df,
        vertex_col="id",
        vertex_time_col="time",
        vertex_props=["name"],
        vertex_const_props=["type"],
    )
    assert g.vertices().properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]
    assert g.layers(["layer 1"]).edges().src().id().collect() == [1]
    assert g.layers(["layer 1", "layer 2"]).edges().src().id().collect() == [1, 2]
    assert g.layers(["layer 1", "layer 2", "layer 3"]).edges().src().id().collect() == [
        1,
        2,
        3,
    ]
    assert g.layers(["layer 1", "layer 4", "layer 5"]).edges().src().id().collect() == [
        1,
        4,
        5,
    ]

    g = Graph.load_from_pandas(
        edges_df,
        src="src",
        dst="dst",
        time="time",
        props=["weight", "marbles"],
        vertex_df=vertices_df,
        vertex_col="id",
        vertex_time_col="time",
        vertex_props=["name"],
        layer_in_df="layers",
    )

    g.load_vertex_props_from_pandas(
        vertices_df, "id", const_props=["type"], shared_const_props={"tag": "test_tag"}
    )
    assert g.vertices().properties.constant.get("type").collect() == [
        "Person 1",
        "Person 2",
        "Person 3",
        "Person 4",
        "Person 5",
        "Person 6",
    ]
    assert g.vertices().properties.constant.get("tag").collect() == [
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
        layer_in_df="layers",
    )
    assert g.layers(["layer 1", "layer 2", "layer 3"]).edges().properties.constant.get(
        "marbles_const"
    ).collect() == [{"layer 1": "red"}, {"layer 2": "blue"}, {"layer 3": "green"}]
    assert g.edges().properties.constant.get("tag").collect() == [
        {"layer 1": "test_tag"},
        {"layer 2": "test_tag"},
        {"layer 3": "test_tag"},
        {"layer 4": "test_tag"},
        {"layer 5": "test_tag"},
    ]


def test_edge_layer():
    g = Graph()
    g.add_edge(1, 1, 2, layer="layer 1").add_constant_properties(
        {"test_prop": "test_val"}
    )
    g.add_edge(1, 2, 3, layer="layer 2").add_constant_properties(
        {"test_prop": "test_val 2"}
    )
    assert g.edges().properties.constant.get("test_prop") == [
        {"layer 1": "test_val"},
        {"layer 2": "test_val 2"},
    ]


def test_edge_explode_layers():
    g = Graph()
    g.add_edge(1, 1, 2, {"layer": 1}, layer="1")
    g.add_edge(1, 1, 2, {"layer": 2}, layer="2")
    g.add_edge(1, 2, 1, {"layer": 1}, layer="1")
    g.add_edge(1, 2, 1, {"layer": 2}, layer="2")

    layered_edges = g.edge(1, 2).explode_layers()
    e_layers = [ee.layer_names() for ee in layered_edges]
    e_layer_prop = [[str(ee.properties["layer"])] for ee in layered_edges]
    assert e_layers == e_layer_prop
    print(e_layers)

    nested_layered_edges = g.vertices.out_edges().explode_layers()
    e_layers = [[ee.layer_names() for ee in edges] for edges in nested_layered_edges]
    e_layer_prop = [
        [[str(ee.properties["layer"])] for ee in layered_edges]
        for layered_edges in nested_layered_edges
    ]
    assert e_layers == e_layer_prop
    print(e_layers)

    print(g.vertices.out_neighbours().collect())
    nested_layered_edges = g.vertices.out_neighbours().out_edges().explode_layers()
    print(nested_layered_edges)
    e_layers = [
        [ee.layer_names() for ee in layered_edges]
        for layered_edges in nested_layered_edges
    ]
    e_layer_prop = [
        [[str(ee.properties["layer"])] for ee in layered_edges]
        for layered_edges in nested_layered_edges
    ]
    assert e_layers == e_layer_prop
    print(e_layers)


def test_hits_algorithm():
    g = graph_loader.lotr_graph()
    assert algorithms.hits(g).get("Aldor") == (
        0.0035840950440615416,
        0.007476256228983402,
    )


def test_balance_algorithm():
    g = Graph()
    edges_str = [
        ("1", "2", 10.0, 1),
        ("1", "4", 20.0, 2),
        ("2", "3", 5.0, 3),
        ("3", "2", 2.0, 4),
        ("3", "1", 1.0, 5),
        ("4", "3", 10.0, 6),
        ("4", "1", 5.0, 7),
        ("1", "5", 2.0, 8),
    ]
    for src, dst, val, time in edges_str:
        g.add_edge(time, src, dst, {"value_dec": val})
    result = algorithms.balance(g, "value_dec", PyDirection("BOTH"), None).get_all()
    assert result == {"1": -26.0, "2": 7.0, "3": 12.0, "4": 5.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", PyDirection("IN"), None).get_all()
    assert result == {"1": 6.0, "2": 12.0, "3": 15.0, "4": 20.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", PyDirection("OUT"), None).get_all()
    assert result == {"1": -32.0, "2": -5.0, "3": -3.0, "4": -15.0, "5": 0.0}
