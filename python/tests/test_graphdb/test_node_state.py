from raphtory import Graph


def test_degree_window():
    g = Graph()
    g.add_edge(0, 1, 2)
    g.add_edge(1, 1, 3)
    g.add_edge(2, 1, 4)

    degs = g.nodes.out_degree()
    assert degs == [3, 0, 0, 0]
    assert degs.before(1) == [1, 0, 0, 0]
    assert degs[1] == 3
    assert degs.before(1)[1] == 1


def test_degree_layer():
    g = Graph()
    g.add_edge(0, 1, 2, layer="1")
    g.add_edge(0, 1, 3, layer="2")
    g.add_edge(0, 1, 4, layer="2")

    degs = g.nodes.out_degree()
    assert degs == [3, 0, 0, 0]
    assert degs.layers(["1"]) == [1, 0, 0, 0]
    assert degs.layers(["2"]) == [2, 0, 0, 0]
