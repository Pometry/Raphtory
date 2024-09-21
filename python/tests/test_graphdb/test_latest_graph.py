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


def test_node_latest():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(10, 1, 2)
    g.add_edge(30, 1, 3)
    assert g.node(1).latest().is_active()
    assert not g.node(2).latest().is_active()

    wg = g.window(5,12)
    print()
    print(wg.node(1).latest().history())
    print(wg.node(1).latest().earliest_time)
    print(wg.node(1).latest().latest_time)
    #assert wg.node(1).latest().is_active()
    #assert not wg.node(2).latest().is_active()
