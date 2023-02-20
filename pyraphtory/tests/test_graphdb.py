import sys
import pyraphtory
from pyraphtory import GraphDB
from pyraphtory import Direction


def create_graph(num_shards):
    g = GraphDB(num_shards)

    edges = [
        (1, 2, 1),
        (1, 3, 2),
        (2, 1, -1),
        (1, 1, 0),
        (3, 2, 7),
        (1, 1, 1)
    ]

    for e in edges:
        g.add_edge(e[0], e[1], e[2], {})

    return g


def test_graph_len_edge_len():
    g = create_graph(2)

    assert g.len() == 3
    assert g.edges_len() == 5


def test_graph_contains():
    g = create_graph(2)

    assert g.contains(3)


def test_graph_degree():
    g = create_graph(3)

    indegree = g.degree(1, pyraphtory.Direction.IN)
    outdegree = g.degree(2, pyraphtory.Direction.OUT)
    degree = g.degree(3, pyraphtory.Direction.BOTH)

    assert indegree == 2
    assert outdegree == 1
    assert degree == 2


def test_graph_degree_window():
    g = create_graph(3)

    indegree_w = g.degree_window(
        1, 0, sys.maxsize, pyraphtory.Direction.IN)
    outdegree_w = g.degree_window(
        2, 0, sys.maxsize, pyraphtory.Direction.OUT)
    degree_w = g.degree_window(
        3, 0, sys.maxsize, pyraphtory.Direction.BOTH)

    assert indegree_w == 1
    assert outdegree_w == 0
    assert degree_w == 2


def test_graph_neighbours():
    g = create_graph(1)

    in_neighbours = []
    for e in g.neighbours(1, pyraphtory.Direction.IN):
        in_neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert in_neighbours == [
        [1, 1, None, False],
        [2, 1, None, False]
    ]

    out_neighbours = []
    for e in g.neighbours(2, pyraphtory.Direction.OUT):
        out_neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert out_neighbours == [
        [2, 1, None, False]
    ]

    neighbours = []
    for e in g.neighbours(3, pyraphtory.Direction.BOTH):
        neighbours.append([e.src, e.dst, e.t, e.is_remote])
    assert neighbours == [
        [1, 3, None, False],
        [3, 2, None, False]
    ]


def test_graph_neighbours_window():
    g = create_graph(3)

    in_neighbours_w = []
    for e in g.neighbours_window(1, 0, sys.maxsize, Direction.IN):
        in_neighbours_w.append([e.src, e.dst, e.t, e.is_remote])
    assert in_neighbours_w == [
        [1, 1, None, False]
    ]

    out_neighbours_w = []
    for e in g.neighbours_window(2, 0, sys.maxsize, Direction.OUT):
        out_neighbours_w.append([e.src, e.dst, e.t, e.is_remote])
    assert out_neighbours_w == []

    neighbours_w = []
    for e in g.neighbours_window(3, 0, sys.maxsize, Direction.BOTH):
        neighbours_w.append([e.src, e.dst, e.t, e.is_remote])
    assert neighbours_w == [
        [1, 0, None, True],
        [0, 2, None, True]
    ]


def test_graph_neighbours_window_t():
    g = create_graph(3)

    in_neighbours_w_t = []
    for e in g.neighbours_window_t(1, 0, sys.maxsize, Direction.IN):
        in_neighbours_w_t.append([e.src, e.dst, e.t, e.is_remote])
    assert in_neighbours_w_t == [
        [1, 1, 0, False],
        [1, 1, 1, False]
    ]

    out_neighbours_w_t = []
    for e in g.neighbours_window_t(2, 0, sys.maxsize, Direction.OUT):
        out_neighbours_w_t.append([e.src, e.dst, e.t, e.is_remote])
    assert out_neighbours_w_t == []

    neighbours_w_t = []
    for e in g.neighbours_window_t(3, 0, sys.maxsize, Direction.BOTH):
        neighbours_w_t.append([e.src, e.dst, e.t, e.is_remote])
    assert neighbours_w_t == [[1, 0, 2, True], [0, 2, 7, True]]


def test_graph_vertices():
    g = create_graph(3)

    vs = [v for v in g.vertices()]
    vs.sort()

    assert vs == [1, 2, 3]
