import re
import sys

import pytest
from raphtory import Graph
from raphtory import algorithms
from raphtory import graph_loader
import tempfile
from math import isclose


def create_graph(num_shards):
    g = Graph(num_shards)
    edges = [
        (1, 1, 2),
        (2, 1, 3),
        (-1, 2, 1),
        (0, 1, 1),
        (7, 3, 2),
        (1, 1, 1)
    ]

    g.add_vertex(0, 1, {"type": "wallet", "cost": 99.5})
    g.add_vertex(-1, 2, {"type": "wallet", "cost": 10.0})
    g.add_vertex(6, 3, {"type": "wallet", "cost": 76})

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {"prop1": 1,
                                      "prop2": 9.8, "prop3": "test"})

    return g


def test_graph_len_edge_len():
    g = create_graph(2)

    assert g.num_vertices() == 3
    assert g.num_edges() == 5


def test_id_iterable():
    g = create_graph(2)

    assert g.vertices.id().max() == 3
    assert g.vertices.id().min() == 1
    assert set(g.vertices.id().collect()) == {1, 2, 3}
    out_neighbours = g.vertices.out_neighbours().id().collect()
    out_neighbours = (set(n) for n in out_neighbours)
    out_neighbours = dict(zip(g.vertices.id(), out_neighbours))

    assert out_neighbours == {1: {1, 2, 3}, 2: {1}, 3: {2}}


def test_degree_iterable():
    g = create_graph(2)
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
    g = create_graph(2)

    assert g.vertices.earliest_time().min() == -1
    assert g.vertices.latest_time().max() == 7


def test_graph_has_edge():
    g = create_graph(2)

    assert not g.window(-1, 1).has_edge(1, 3)
    assert g.window(-1, 3).has_edge(1, 3)
    assert not g.window(10, 11).has_edge(1, 3)


def test_graph_has_vertex():
    g = create_graph(2)

    assert g.has_vertex(3)


def test_windowed_graph_has_vertex():
    g = create_graph(2)

    assert g.window(-1, 1).has_vertex(1)


def test_windowed_graph_get_vertex():
    g = create_graph(2)

    view = g.window(0, sys.maxsize)

    assert view.vertex(1).id() == 1
    assert view.vertex(10) is None
    assert view.vertex(1).degree() == 3


def test_windowed_graph_degree():
    g = create_graph(3)

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
    g = create_graph(2)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert (view.edge(1, 3).src().id(), view.edge(1, 3).dst().id()) == (1, 3)
    assert view.edge(2, 3) == None
    assert view.edge(6, 5) == None

    assert (view.vertex(1).id(), view.vertex(3).id()) == (1, 3)

    view = g.window(2, 3)
    assert (view.edge(1, 3).src().id(), view.edge(1, 3).dst().id()) == (1, 3)

    view = g.window(3, 7)
    assert view.edge(1, 3) == None


def test_windowed_graph_edges():
    g = create_graph(1)

    view = g.window(0, sys.maxsize)

    tedges = [v.edges() for v in view.vertices()]
    edges = []
    for e_iter in tedges:
        for e in e_iter:
            edges.append([e.src().id(), e.dst().id()])

    assert edges == [
        [1, 1],
        [1, 1],
        [1, 2],
        [1, 3],
        [1, 2],
        [3, 2],
        [1, 3],
        [3, 2]
    ]

    tedges = [v.in_edges() for v in view.vertices()]
    in_edges = []
    for e_iter in tedges:
        for e in e_iter:
            in_edges.append([e.src().id(), e.dst().id()])

    assert in_edges == [
        [1, 1],
        [1, 2],
        [3, 2],
        [1, 3]
    ]

    tedges = [v.out_edges() for v in view.vertices()]
    out_edges = []
    for e_iter in tedges:
        for e in e_iter:
            out_edges.append([e.src().id(), e.dst().id()])

    assert out_edges == [
        [1, 1],
        [1, 2],
        [1, 3],
        [3, 2]
    ]


def test_windowed_graph_vertex_ids():
    g = create_graph(3)

    vs = [v for v in g.window(-1, 2).vertices().id()]
    vs.sort()
    assert vs == [1, 2]  # this makes clear that the end of the range is exclusive

    vs = [v for v in g.window(-5, 3).vertices().id()]
    vs.sort()
    assert vs == [1, 2, 3]


def test_windowed_graph_vertices():
    g = create_graph(1)

    view = g.window(-1, 0)

    vertices = list(view.vertices().id())

    assert vertices == [1, 2]


def test_windowed_graph_neighbours():
    g = create_graph(1)

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
    # Currently deadlocking
    g = Graph()
    g.add_vertex(1, "Ben")
    g.add_vertex(1, 10)
    g.add_edge(1, "Ben", "Hamza")
    assert g.vertex(10).name() == "10"
    assert g.vertex("Ben").name() == "Ben"


# assert g.vertex("Hamza").name() == "Hamza" TODO need to fix


def test_vertex_properties():
    g = Graph()
    g.add_edge(1, 1, 1)
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    g.add_vertex(1, 1, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    g.add_vertex(2, 1, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    g.add_vertex(3, 1, props_t3)

    g.add_vertex_properties(1, {"static prop": 123})

    # testing property_history
    def history_test(key, value):
        assert g.vertex(1).property_history(key) == value
        assert g.vertices.property_history(key).collect() == [value]
        assert g.vertices.out_neighbours().property_history(key).collect() == [[value]]

    history_test("prop 1", [(1, 1), (2, 2)])
    history_test("prop 2", [(2, 0.6), (3, 0.9)])
    history_test("prop 3", [(1, "hi"), (3, 'hello')])
    history_test("prop 4", [(1, True), (2, False), (3, True)])
    history_test("undefined", [])

    def time_history_test(time, key, value):
        assert g.at(time).vertex(1).property_history(key) == value
        assert g.at(time).vertices.property_history(key).collect() == [value]
        assert g.at(time).vertices.out_neighbours().property_history(key).collect() == [[value]]

    time_history_test(1, "prop 4", [(1, True)])
    time_history_test(1, "static prop", [])

    def time_static_property_test(time, key, value):
        gg = g.at(time)
        assert gg.vertex(1).static_property(key) == value
        assert gg.vertices.static_property(key).collect() == [value]
        assert gg.vertices.out_neighbours().static_property(key).collect() == [[value]]

    def static_property_test(key, value):
        assert g.vertex(1).static_property(key) == value
        assert g.vertices.static_property(key).collect() == [value]
        assert g.vertices.out_neighbours().static_property(key).collect() == [[value]]

    time_static_property_test(1, "static prop", 123)
    time_static_property_test(100, "static prop", 123)
    static_property_test("static prop", 123)
    static_property_test("prop 4", None)

    # testing property
    def time_property_test(time, key, value):
        gg = g.at(time)
        assert gg.vertex(1).property(key) == value
        assert gg.vertices.property(key).collect() == [value]
        assert gg.vertices.out_neighbours().property(key).collect() == [[value]]

    def property_test(key, value):
        assert g.vertex(1).property(key) == value
        assert g.vertices.property(key).collect() == [value]
        assert g.vertices.out_neighbours().property(key).collect() == [[value]]

    def no_static_property_test(key, value):
        assert g.vertex(1).property(key, include_static=False) == value
        assert g.vertices.property(key, include_static=False).collect() == [value]
        assert g.vertices.out_neighbours().property(key, include_static=False).collect() == [[value]]

    property_test("static prop", 123)
    assert g.vertex(1)["static prop"] == 123
    no_static_property_test("static prop", None)
    no_static_property_test("prop 1", 2)
    time_property_test(2, "prop 2", 0.6)
    time_property_test(1, "prop 2", None)

    # testing properties
    assert g.vertex(1).properties() == {'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2, 'prop 4': True,
                                        'static prop': 123}
    assert g.vertices.properties().collect() == [{'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2, 'prop 4': True,
                                                  'static prop': 123}]
    assert g.vertices.out_neighbours().properties().collect() == [[
        {'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2, 'prop 4': True,
         'static prop': 123}]]

    assert g.vertex(1).properties(include_static=False) == {'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2,
                                                            'prop 4': True}
    assert g.vertices.properties(include_static=False).collect() == [{'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2,
                                                            'prop 4': True}]
    assert g.vertices.out_neighbours().properties(include_static=False).collect() == [[{'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2,
                                                            'prop 4': True}]]

    assert g.at(2).vertex(1).properties() == {'prop 1': 2, 'prop 4': False, 'prop 2': 0.6, 'static prop': 123,
                                              'prop 3': 'hi'}
    assert g.at(2).vertices.properties().collect() == [{'prop 1': 2, 'prop 4': False, 'prop 2': 0.6, 'static prop': 123,
                                              'prop 3': 'hi'}]
    assert g.at(2).vertices.out_neighbours().properties().collect() == [[{'prop 1': 2, 'prop 4': False, 'prop 2': 0.6, 'static prop': 123,
                                              'prop 3': 'hi'}]]

    # testing property histories
    assert g.vertex(1).property_histories() == {'prop 3': [(1, 'hi'), (3, 'hello')], 'prop 1': [(1, 1), (2, 2)],
                                                'prop 4': [(1, True), (2, False), (3, True)],
                                                'prop 2': [(2, 0.6), (3, 0.9)]}
    assert g.vertices.property_histories().collect() == [{'prop 3': [(1, 'hi'), (3, 'hello')], 'prop 1': [(1, 1), (2, 2)],
                                                'prop 4': [(1, True), (2, False), (3, True)],
                                                'prop 2': [(2, 0.6), (3, 0.9)]}]
    assert g.vertices.out_neighbours().property_histories().collect() == [[{'prop 3': [(1, 'hi'), (3, 'hello')], 'prop 1': [(1, 1), (2, 2)],
                                                'prop 4': [(1, True), (2, False), (3, True)],
                                                'prop 2': [(2, 0.6), (3, 0.9)]}]]

    assert g.at(2).vertex(1).property_histories() == {'prop 2': [(2, 0.6)], 'prop 4': [(1, True), (2, False)],
                                                      'prop 1': [(1, 1), (2, 2)], 'prop 3': [(1, 'hi')]}
    assert g.at(2).vertices.property_histories().collect() == [{'prop 2': [(2, 0.6)], 'prop 4': [(1, True), (2, False)],
                                                      'prop 1': [(1, 1), (2, 2)], 'prop 3': [(1, 'hi')]}]
    assert g.at(2).vertices.out_neighbours().property_histories().collect() == [[{'prop 2': [(2, 0.6)], 'prop 4': [(1, True), (2, False)],
                                                      'prop 1': [(1, 1), (2, 2)], 'prop 3': [(1, 'hi')]}]]

    # testing property names
    expected_names = sorted(['prop 4', 'prop 1', 'prop 2', 'prop 3', 'static prop'])
    assert sorted(g.vertex(1).property_names()) == expected_names
    names = g.vertices.property_names().collect()
    assert len(names) == 1 and sorted(names[0]) == expected_names
    names = g.vertices.out_neighbours().property_names().collect()
    assert len(names) == 1 and len(names[0]) == 1 and sorted(names[0][0]) == expected_names

    expected_names_no_static = sorted(['prop 4', 'prop 1', 'prop 2', 'prop 3'])
    assert sorted(g.vertex(1).property_names(include_static=False)) == expected_names_no_static
    names = g.vertices.property_names(include_static=False).collect()
    assert len(names) == 1 and sorted(names[0]) == expected_names_no_static
    names = g.vertices.out_neighbours().property_names(include_static=False).collect()
    assert len(names) == 1 and len(names[0]) == 1 and sorted(names[0][0]) == expected_names_no_static

    assert sorted(g.at(1).vertex(1).property_names(include_static=False)) == expected_names_no_static
    names = g.at(1).vertices.property_names(include_static=False).collect()
    assert len(names) == 1 and sorted(names[0]) == expected_names_no_static
    names = g.at(1).vertices.out_neighbours().property_names(include_static=False).collect()
    assert len(names) == 1 and len(names[0]) == 1 and sorted(names[0][0]) == expected_names_no_static

    # testing has_property
    assert g.vertex(1).has_property("prop 4")
    assert g.vertices.has_property("prop 4").collect() == [True]
    assert g.vertices.out_neighbours().has_property("prop 4").collect() == [[True]]

    assert g.vertex(1).has_property("prop 2")
    assert g.vertices.has_property("prop 2").collect() == [True]
    assert g.vertices.out_neighbours().has_property("prop 2").collect() == [[True]]

    assert not g.vertex(1).has_property("prop 5")
    assert g.vertices.has_property("prop 5").collect() == [False]
    assert g.vertices.out_neighbours().has_property("prop 5").collect() == [[False]]

    assert not g.at(1).vertex(1).has_property("prop 2")
    assert g.at(1).vertices.has_property("prop 2").collect() == [False]
    assert g.at(1).vertices.out_neighbours().has_property("prop 2").collect() == [[False]]

    assert g.vertex(1).has_property("static prop")
    assert g.vertices.has_property("static prop").collect() == [True]
    assert g.vertices.out_neighbours().has_property("static prop").collect() == [[True]]

    assert g.at(1).vertex(1).has_property("static prop")
    assert g.at(1).vertices.has_property("static prop").collect() == [True]
    assert g.at(1).vertices.out_neighbours().has_property("static prop").collect() == [[True]]

    assert not g.at(1).vertex(1).has_property("static prop", include_static=False)
    assert g.at(1).vertices.has_property("static prop", include_static=False).collect() == [False]
    assert g.at(1).vertices.out_neighbours().has_property("static prop", include_static=False).collect() == [[False]]

    assert g.vertex(1).has_static_property("static prop")
    assert g.vertices.has_static_property("static prop").collect() == [True]
    assert g.vertices.out_neighbours().has_static_property("static prop").collect() == [[True]]

    assert not g.vertex(1).has_static_property("prop 2")
    assert g.vertices.has_static_property("prop 2").collect() == [False]
    assert g.vertices.out_neighbours().has_static_property("prop 2").collect() == [[False]]

    assert g.at(1).vertex(1).has_static_property("static prop")
    assert g.at(1).vertices.has_static_property("static prop").collect() == [True]
    assert g.at(1).vertices.out_neighbours().has_static_property("static prop").collect() == [[True]]


def test_edge_properties():
    g = Graph()
    props_t1 = {"prop 1": 1, "prop 3": "hi", "prop 4": True}
    g.add_edge(1, 1, 2, props_t1)
    props_t2 = {"prop 1": 2, "prop 2": 0.6, "prop 4": False}
    g.add_edge(2, 1, 2, props_t2)
    props_t3 = {"prop 2": 0.9, "prop 3": "hello", "prop 4": True}
    g.add_edge(3, 1, 2, props_t3)

    g.add_edge_properties(1, 2, {"static prop": 123})

    # testing property_history
    assert g.edge(1, 2).property_history("prop 1") == [(1, 1), (2, 2)]
    assert g.edge(1, 2).property_history("prop 2") == [(2, 0.6), (3, 0.9)]
    assert g.edge(1, 2).property_history("prop 3") == [(1, "hi"), (3, 'hello')]
    assert g.edge(1, 2).property_history("prop 4") == [(1, True), (2, False), (3, True)]
    assert g.edge(1, 2).property_history("undefined") == []
    assert g.at(1).edge(1, 2).property_history("prop 4") == [(1, True)]
    assert g.at(1).edge(1, 2).property_history("static prop") == []

    assert g.at(1).edge(1, 2).static_property("static prop") == 123
    assert g.at(100).edge(1, 2).static_property("static prop") == 123
    assert g.edge(1, 2).static_property("static prop") == 123
    assert g.edge(1, 2).static_property("prop 4") is None

    # testing property
    assert g.edge(1, 2).property("static prop") == 123
    assert g.edge(1, 2)["static prop"] == 123
    assert g.edge(1, 2).property("static prop", include_static=False) is None
    assert g.edge(1, 2).property("prop 1", include_static=False) == 2
    assert g.at(2).edge(1, 2).property("prop 2") == 0.6
    assert g.at(1).edge(1, 2).property("prop 2") is None

    # testing properties
    assert g.edge(1, 2).properties() == {'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2, 'prop 4': True,
                                         'static prop': 123}

    assert g.edge(1, 2).properties(include_static=False) == {'prop 2': 0.9, 'prop 3': 'hello', 'prop 1': 2,
                                                             'prop 4': True}

    assert g.at(2).edge(1, 2).properties() == {'prop 1': 2, 'prop 4': False, 'prop 2': 0.6, 'static prop': 123,
                                               'prop 3': 'hi'}

    # testing property histories
    assert g.edge(1, 2).property_histories() == {'prop 3': [(1, 'hi'), (3, 'hello')], 'prop 1': [(1, 1), (2, 2)],
                                                 'prop 4': [(1, True), (2, False), (3, True)],
                                                 'prop 2': [(2, 0.6), (3, 0.9)]}

    assert g.at(2).edge(1, 2).property_histories() == {'prop 2': [(2, 0.6)], 'prop 4': [(1, True), (2, False)],
                                                       'prop 1': [(1, 1), (2, 2)], 'prop 3': [(1, 'hi')]}

    # testing property names
    assert g.edge(1, 2).property_names().sort() == ['prop 4', 'prop 1', 'prop 2', 'prop 3', 'static prop'].sort()

    assert g.edge(1, 2).property_names(include_static=False).sort() == ['prop 4', 'prop 1', 'prop 2', 'prop 3'].sort()

    assert g.at(1).edge(1, 2).property_names(include_static=False).sort() == ['prop 4', 'prop 1', 'prop 2',
                                                                              'prop 3'].sort()

    # testing has_property
    assert g.edge(1, 2).has_property("prop 4")
    assert g.edge(1, 2).has_property("prop 2")
    assert not g.edge(1, 2).has_property("prop 5")
    assert not g.at(1).edge(1, 2).has_property("prop 2")
    assert g.edge(1, 2).has_property("static prop")
    assert g.at(1).edge(1, 2).has_property("static prop")
    assert not g.at(1).edge(1, 2).has_property("static prop", include_static=False)

    assert g.edge(1, 2).has_static_property("static prop")
    assert not g.edge(1, 2).has_static_property("prop 2")
    assert g.at(1).edge(1, 2).has_static_property("static prop")


def test_exploded_edge_time():
    g = graph_loader.lotr_graph()
    e = g.edge("Frodo","Gandalf")
    his = e.history()
    exploded_his = []
    for ee in e.explode():
        exploded_his.append(ee.time())
    assert(his,exploded_his)

# assert g.vertex(1).property_history("prop 3") == [(1, 3), (3, 'hello')]


def test_algorithms():
    g = Graph(1)
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
    
    lotr_clustering_coefficient = algorithms.local_clustering_coefficient(lotr_graph, 'Frodo')
    lotr_local_triangle_count = algorithms.local_triangle_count(lotr_graph, 'Frodo')
    assert lotr_clustering_coefficient == 0.1984313726425171
    assert lotr_local_triangle_count == 253

def test_graph_time_api():
    g = create_graph(1)

    earliest_time = g.earliest_time()
    latest_time = g.latest_time()
    assert len(list(g.rolling(1))) == latest_time - earliest_time + 1
    assert len(list(g.expanding(2))) == (latest_time - earliest_time) / 2

    w = g.window(2, 6)
    assert len(list(w.rolling(window=10, step=3))) == 1


def test_save_load_graph():
    g = create_graph(1)
    g.add_vertex(1, 11, {"type": "wallet", "balance": 99.5})
    g.add_vertex(2, 12, {"type": "wallet", "balance": 10.0})
    g.add_vertex(3, 13, {"type": "wallet", "balance": 76})
    g.add_edge(4, 11, 12, {"prop1": 1, "prop2": 9.8, "prop3": "test"})
    g.add_edge(5, 12, 13, {"prop1": 1321, "prop2": 9.8, "prop3": "test"})
    g.add_edge(6, 13, 11, {"prop1": 645, "prop2": 9.8, "prop3": "test"})

    tmpdirname = tempfile.TemporaryDirectory()
    g.save_to_file(tmpdirname.name)

    del (g)

    g = Graph.load_from_file(tmpdirname.name)

    view = g.window(0, 10)
    assert g.has_vertex(13)
    assert view.vertex(13).in_degree() == 1
    assert view.vertex(13).out_degree() == 1
    assert view.vertex(13).degree() == 2

    triangles = algorithms.local_triangle_count(view, 13)  # How many triangles is 13 involved in
    assert triangles == 1

    v = view.vertex(11)
    assert v.property_histories() == {'type': [(1, 'wallet')], 'balance': [(1, 99.5)]}

    tmpdirname.cleanup()


def test_graph_at():
    g = create_graph(1)

    view = g.at(2)
    assert view.vertex(1).degree() == 3
    assert view.vertex(3).degree() == 1

    view = g.at(7)
    assert view.vertex(3).degree() == 2


def test_add_node_string():
    g = Graph(1)

    g.add_vertex(0, 1, {})
    g.add_vertex(1, "haaroon", {})
    g.add_vertex(1, "haaroon", {})  # add same vertex twice used to cause an exception

    assert g.has_vertex(1)
    assert g.has_vertex("haaroon")


def test_add_edge_string():
    g = Graph(1)

    g.add_edge(0, 1, 2, {})
    g.add_edge(1, "haaroon", "ben", {})

    assert g.has_vertex(1)
    assert g.has_vertex(2)
    assert g.has_vertex("haaroon")
    assert g.has_vertex("ben")

    assert g.has_edge(1, 2)
    assert g.has_edge("haaroon", "ben")


def test_all_neighbours_window():
    g = Graph(4)
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
    g = Graph(4)
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
    g = Graph(4)
    g.add_edge(1, 1, 2, {})
    g.add_edge(1, 2, 3, {})
    g.add_edge(2, 3, 2, {})
    g.add_edge(3, 3, 2, {})
    g.add_edge(3, 4, 2, {})
    g.add_edge(4, 2, 4, {})
    g.add_edge(5, 2, 1, {})

    view = g.at(4)
    v = view.vertex(2)
    assert list(map(lambda e: e.id(), v.window(0, 4).in_edges())) == [1, 3, 5]
    assert list(map(lambda e: e.id(), v.window(t_end=4).in_edges())) == [1, 3, 5]
    assert list(map(lambda e: e.id(), v.window(t_start=2).in_edges())) == [3, 5]
    assert list(map(lambda e: e.id(), v.window(0, 4).out_edges())) == [2]
    assert list(map(lambda e: e.id(), v.window(t_end=3).out_edges())) == [2]
    assert list(map(lambda e: e.id(), v.window(t_start=2).out_edges())) == [6]
    assert sorted(list(map(lambda e: e.id(), v.window(0, 4).edges()))) == [1, 2, 3, 5]
    assert sorted(list(map(lambda e: e.id(), v.window(t_end=4).edges()))) == [1, 2, 3, 5]
    assert sorted(list(map(lambda e: e.id(), v.window(t_start=1).edges()))) == [1, 2, 3, 5, 6]


def test_static_prop_change():
    # with pytest.raises(Exception):
    g = Graph(1)

    g.add_edge(0, 1, 2, {})
    g.add_vertex_properties(1, {"name": "value1"})

    expected_msg = (
        """Exception: Failed to mutate graph\n"""
        """Caused by:\n"""
        """  -> cannot change property for vertex '1'\n"""
        """  -> cannot mutate static property 'name'\n"""
        """  -> cannot set previous value 'Some(Str("value1"))' to 'Some(Str("value2"))' in position '0'"""
    )

    # with pytest.raises(Exception, match=re.escape(expected_msg)):
    with pytest.raises(Exception):
        g.add_vertex_properties(1, {"name": "value2"})


def test_triplet_count():
    g = Graph(1)

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})

    v = g.at(1)
    assert algorithms.triplet_count(v) == 3


def test_global_clustering_coeffficient():
    g = Graph(1)

    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 1, {})
    g.add_edge(0, 4, 2, {})
    g.add_edge(0, 4, 1, {})
    g.add_edge(0, 5, 2, {})

    v = g.at(1)
    assert algorithms.global_clustering_coefficient(v) == 0.5454545454545454


def test_edge_time_apis():
    g = Graph(1)

    g.add_edge(1, 1, 2, {"prop2": 10})
    g.add_edge(2, 2, 4, {"prop2": 11})
    g.add_edge(3, 4, 5, {"prop2": 12})
    g.add_edge(4, 1, 5, {"prop2": 13})

    v = g.vertex(1)
    e = g.edge(1, 2)

    for e in e.expanding(1):
        assert e.src().name() == '1'
        assert e.dst().name() == '2'

    ls = []
    for e in v.edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ['1', '2', '1', '5']

    v = g.vertex(2)
    ls = []
    for e in v.in_edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ['1', '2']

    ls = []
    for e in v.out_edges():
        ls.append(e.src().name())
        ls.append(e.dst().name())

    assert ls == ['2', '4']


def test_edge_earliest_latest_time():
    g = Graph(1)
    g.add_edge(0, 1, 2, {})
    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(1, 1, 3, {})
    g.add_edge(2, 1, 3, {})


    assert g.edge(1, 2).earliest_time() == 0
    assert g.edge(1, 2).latest_time() == 2

    assert g.vertex(1).edges().earliest_time() == [0, 0]
    assert g.vertex(1).edges().latest_time() == [2, 2]
    assert g.vertex(1).at(1).edges().earliest_time() == [0, 0]
    assert g.vertex(1).at(1).edges().latest_time() == [1, 1]


def test_vertex_history():
    g = Graph(1)

    g.add_vertex(1, 1, {})
    g.add_vertex(2, 1, {})
    g.add_vertex(3, 1, {})
    g.add_vertex(4, 1, {})
    g.add_vertex(8, 1, {})

    g.add_vertex(4, "Lord Farquaad", {})
    g.add_vertex(6, "Lord Farquaad", {})
    g.add_vertex(7, "Lord Farquaad", {})
    g.add_vertex(8, "Lord Farquaad", {})
                 
    assert(g.vertex(1).history() == [1, 2, 3, 4, 8])
    assert(g.vertex("Lord Farquaad").history() == [4, 6, 7, 8])

    view = g.window(1, 8)

    assert(view.vertex(1).history() == [1, 2 ,3 ,4])
    assert(view.vertex("Lord Farquaad").history() == [4, 6, 7])

def test_edge_history():
    g = Graph(1)

    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 1, 3, {})
    g.add_edge(3, 1, 2, {})
    g.add_edge(4, 1, 4, {})

    view = g.window(1, 5)

    assert(g.edge(1,2).history() == [1,3])

    # also needs to be fixed in Pedros PR
    # assert(view.edge(1, 4).history() == [4])

def test_lotr_edge_history():
    g = graph_loader.lotr_graph()
    
    assert(g.edge('Frodo','Gandalf').history() == [329, 555, 861, 1056, 1130, 1160, 1234, 1241, 1390, 1417, 1656, 1741, 1783, 1785, 1792, 1804, 1809, 1999, 2056, 2254, 2925, 2999, 3703, 3914, 4910, 5620, 5775, 6381, 6531, 6578, 6661, 6757, 7041, 7356, 8183, 8190, 8276, 8459, 8598, 8871, 9098, 9343, 9903, 11189, 11192, 11279, 11365, 14364, 21551, 21706, 23212, 26958, 27060, 29024, 30173, 30737, 30744, 31023, 31052, 31054, 31103, 31445, 32656])
    assert(g.at(1000).edge('Frodo','Gandalf').history() == [329, 555, 861])
    assert(g.edge('Frodo','Gandalf').at(1000).history() == [329, 555, 861])
    assert(g.window(100,1000).edge('Frodo','Gandalf').history() == [329, 555, 861])
    assert(g.edge('Frodo','Gandalf').window(100,1000).history() == [329, 555, 861])