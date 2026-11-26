from raphtory import Graph


def test_degree_window():
    g = Graph()
    g.add_edge(0, 1, 2)
    g.add_edge(1, 1, 3)
    g.add_edge(2, 1, 4)

    assert g.nodes.out_degree() == [3, 0, 0, 0]
    assert g.nodes.before(1).out_degree() == [1, 0, 0, 0]
    assert g.nodes.out_degree()[1] == 3
    assert g.nodes.before(1).out_degree()[1] == 1


def test_degree_layer():
    g = Graph()
    g.add_edge(0, 1, 2, layer="1")
    g.add_edge(0, 1, 3, layer="2")
    g.add_edge(0, 1, 4, layer="2")

    assert g.nodes.out_degree() == [3, 0, 0, 0]
    assert g.nodes.layers(["1"]).out_degree() == [1, 0, 0, 0]
    assert g.nodes.layers(["2"]).out_degree() == [2, 0, 0, 0]


def test_group_by():
    g = Graph()
    g.add_edge(0, 1, 2)
    g.add_edge(0, 2, 3)
    g.add_edge(0, 4, 5)

    groups_from_lazy = g.nodes.out_degree().groups()
    groups_from_eager = g.nodes.out_degree().compute().groups()

    expected = {
        0: [3, 5],
        1: [1, 2, 4],
    }

    assert {v: nodes.id.sorted() for v, nodes in groups_from_lazy} == expected

    assert {v: nodes.id.sorted() for v, nodes in groups_from_eager} == expected

    assert {
        v: graph.nodes.id.sorted() for v, graph in groups_from_lazy.iter_subgraphs()
    } == {0: [], 1: [1, 2]}

    assert len(groups_from_lazy) == len(expected)

    for i, (v, nodes) in enumerate(groups_from_lazy):
        v2, nodes2 = groups_from_lazy[i]
        assert v == v2
        assert nodes.id == nodes2.id


def test_node_states_of_two_graphs_compare_by_node_id():
    g = Graph()
    g.add_edge(1, "a", "b")
    g.add_edge(1, "c", "d")
    h = Graph()
    h.add_edge(1, "d", "c")
    h.add_edge(1, "b", "a")
    i = Graph()
    i.add_edge(1, "a", "b")
    i.add_edge(1, "b", "c")
    i.add_node(1, "d")

    # same names, added in a different order
    assert g.nodes.name == h.nodes.name
    assert g.nodes.name.compute() == h.nodes.name.compute()
    # different graphs over the same four nodes, never a panic
    assert g.nodes.degree() != i.nodes.degree()
    assert i.nodes.degree() != g.nodes.degree()
