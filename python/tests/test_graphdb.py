import re
import sys

import pytest
from raphtory import Graph
from raphtory import algorithms
from raphtory import Perspective
from raphtory import graph_loader
import tempfile

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
    assert vs == [1, 2] # this makes clear that the end of the range is exclusive

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

    neighbours = [list(v.neighbours().id()) for v in view.vertices()]
    assert neighbours == [[1, 2, 3], [1, 3], [1, 2]]

    in_neighbours = [list(v.in_neighbours().id()) for v in view.vertices()]
    assert in_neighbours == [[1, 2], [1, 3], [1]]

    out_neighbours = [list(v.out_neighbours().id()) for v in view.vertices()]
    assert out_neighbours == [[1, 2, 3], [1], [2]]


def test_windowed_graph_vertex_prop():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert view.vertex(1).prop("type") == [(0, 'wallet')]
    assert view.vertex(1).prop("undefined") == []


def test_windowed_graph_vertex_props():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    assert view.vertex(1).props() == {
        'cost': [(0, 99.5)], 'type': [(0, 'wallet')]}


def test_windowed_graph_edge_prop():
    g = create_graph(1)

    max_size = sys.maxsize
    min_size = -sys.maxsize - 1

    view = g.window(min_size, max_size)

    edge = next(view.vertex(1).edges())

    assert edge.prop("prop1") == [(0, 1), (1, 1)]
    assert edge.prop("prop3") == [(0, 'test'), (1, 'test')]
    assert edge.prop("undefined") == []


def test_algorithms():

    g = Graph(1)

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

def test_perspective_set():
    g = create_graph(1)

    perspectives = [Perspective(start=0, end=2), Perspective(start=4), Perspective(end=6)]
    views = g.through(perspectives)
    assert len(list(views)) == 3

    perspectives = Perspective.rolling(5, start=0, end=4)
    views = g.through(perspectives)
    assert len(list(views)) == 2

    perspectives = Perspective.expanding(5, start=0, end=4)
    views = g.through(perspectives)
    assert len(list(views)) == 2

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

    del(g)

    g = Graph.load_from_file(tmpdirname.name)

    view = g.window(0,10)
    assert g.has_vertex(13)
    assert view.vertex(13).in_degree() == 1
    assert view.vertex(13).out_degree() == 1
    assert view.vertex(13).degree() == 2

    triangles = algorithms.local_triangle_count(view,13) # How many triangles is 13 involved in
    assert triangles == 1

    v = view.vertex(11)
    assert v.props() == {'type': [(1, 'wallet')], 'balance': [(1, 99.5)]}

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
    g.add_vertex(1, "haaroon", {}) # add same vertex twice used to cause an exception

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
    assert list(v.in_neighbours(0, 2).id()) == [1]
    assert list(v.out_neighbours(0, 2).id()) == [3]
    assert list(v.neighbours(0, 2).id()) == [1, 3]

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
    assert v.in_degree(0, 4) == 3
    assert v.in_degree(t_start=2) == 2
    assert v.in_degree(t_end=3) == 2
    assert v.out_degree(0, 4) == 1
    assert v.out_degree(t_start=2) == 1
    assert v.out_degree(t_end=3) == 1
    assert v.degree(0, 4) == 3
    assert v.degree(t_start=2) == 2
    assert v.degree(t_end=3) == 2

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
    assert list(map(lambda e: e.id(),  v.in_edges(0, 4))) == [1, 3, 5]
    assert list(map(lambda e: e.id(),  v.in_edges(t_end=4))) == [1, 3, 5]
    assert list(map(lambda e: e.id(),  v.in_edges(t_start=2))) == [3, 5]
    assert list(map(lambda e: e.id(),  v.out_edges(0, 4))) == [2]
    assert list(map(lambda e: e.id(),  v.out_edges(t_end=3))) == [2]
    assert list(map(lambda e: e.id(),  v.out_edges(t_start=2))) == [6]
    assert sorted(list(map(lambda e: e.id(),  v.edges(0, 4)))) == [1, 2, 3, 5]
    assert sorted(list(map(lambda e: e.id(),  v.edges(t_end=4)))) == [1, 2, 3, 5]
    assert sorted(list(map(lambda e: e.id(),  v.edges(t_start=1)))) == [1, 2, 3, 5, 6]

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
