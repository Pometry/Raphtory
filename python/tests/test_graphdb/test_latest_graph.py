from raphtory import Graph, PersistentGraph


def test_graph_latest():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 4)
    assert g.latest_time == 3
    assert g.latest().latest_time == 3
    assert g.latest().earliest_time == 3
    assert g.latest().edges.id.collect() == [(1, 4)]

    assert g.window(1, 2).latest().edges.id.collect() == [(1, 2)]


def test_edge_latest():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)

    g.add_edge(4, 1, 3)
    g.add_edge(5, 1, 3)
    g.add_edge(6, 1, 3)

    assert g.edge(1, 2).latest().latest_time is None
    assert not g.edge(1, 2).latest().is_active()
    assert g.edge(1, 3).latest().is_active()

    wg = g.window(2, 4)
    assert wg.edge(1, 2).latest().latest_time is 3
    assert wg.edge(1, 2).latest().is_active()

    assert g.edges.latest().earliest_time.collect() == [None, 6]
    assert g.edges.latest().is_active().collect() == [False, True]

    assert wg.edges.latest().earliest_time.collect() == [3]
    assert wg.edges.latest().is_active().collect() == [True]

    assert g.nodes.edges.latest().earliest_time.collect() == [[None, 6], [None], [6]]
    assert g.nodes.edges.latest().is_active().collect() == [
        [False, True],
        [False],
        [True],
    ]

    assert wg.nodes.edges.latest().earliest_time.collect() == [[3], [3]]
    assert wg.nodes.edges.latest().is_active().collect() == [[True], [True]]


def test_node_latest():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(10, 1, 2)
    g.add_edge(30, 1, 3)
    assert g.node(1).latest().is_active()
    assert not g.node(2).latest().is_active()

    wg = g.window(5, 12)
    assert wg.node(1).latest().history() == [10]

    assert g.nodes.latest().id.collect() == [1, 3]
    assert wg.nodes.latest().id.collect() == [1, 2]


def test_persistent_graph_latest():
    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 1, 4)
    g.delete_edge(3, 1, 3)
    assert g.latest_time == 3
    assert g.latest().latest_time == 3
    assert g.latest().earliest_time == 3
    assert g.latest().edges.id.collect() == [(1, 2), (1, 4)]

    wg = g.window(1, 2)

    assert wg.latest().latest_time == 1
    assert wg.latest().earliest_time == 1

    assert wg.latest().edges.id.collect() == [(1, 2)]
    assert wg.latest().edges.id.collect() == [(1, 2)]


def test_persistent_edge_latest():
    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 2)
    g.add_edge(3, 1, 2)

    g.add_edge(4, 1, 3)
    g.add_edge(5, 1, 3)
    g.add_edge(6, 1, 3)

    g.add_edge(4, 1, 4)
    g.add_edge(5, 1, 4)
    g.delete_edge(6, 1, 4)

    assert g.edge(1, 2).latest().latest_time is 6
    assert g.edge(1, 2).latest().is_active()
    assert g.edge(1, 3).latest().is_active()
    assert not g.edge(1, 4).latest().is_active()

    wg = g.window(3, 6)
    assert wg.edge(1, 2).latest().latest_time is 5
    assert wg.edge(1, 2).latest().is_active()
    assert wg.edge(1, 4).latest().is_active()

    assert g.edges.latest().earliest_time.collect() == [6, 6, None]
    assert g.edges.latest().latest_time.collect() == [6, 6, None]
    assert g.edges.latest().is_active().collect() == [True, True, False]

    assert wg.edges.latest().earliest_time.collect() == [5, 5, 5]
    assert wg.edges.latest().latest_time.collect() == [5, 5, 5]
    assert wg.edges.latest().is_active().collect() == [True, True, True]

    assert g.nodes.edges.latest().earliest_time.collect() == [
        [6, 6, None],
        [6],
        [6],
        [None],
    ]
    assert g.nodes.edges.latest().latest_time.collect() == [
        [6, 6, None],
        [6],
        [6],
        [None],
    ]
    assert g.nodes.edges.latest().is_active().collect() == [
        [True, True, False],
        [True],
        [True],
        [False],
    ]

    assert wg.nodes.edges.latest().earliest_time.collect() == [[5, 5, 5], [5], [5], [5]]
    assert wg.nodes.edges.latest().latest_time.collect() == [[5, 5, 5], [5], [5], [5]]
    assert wg.nodes.edges.latest().is_active().collect() == [
        [True, True, True],
        [True],
        [True],
        [True],
    ]


def test_persistent_node_latest():
    g = PersistentGraph()
    g.add_edge(1, 1, 2)
    g.add_edge(10, 1, 2)
    g.add_edge(30, 1, 3)
    assert g.node(1).latest().is_active()
    assert not g.node(2).latest().is_active()

    wg = g.window(5, 12)
    assert wg.latest_time == 11
    assert wg.earliest_time == 5
    assert wg.node(1).latest().history() == []

    assert g.nodes.latest().id.collect() == [1, 2, 3]
    assert wg.nodes.latest().id.collect() == [1, 2]
