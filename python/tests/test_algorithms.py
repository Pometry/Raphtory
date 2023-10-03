def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality
    g = Graph()
    g.add_edge(0, 0, 1, {})
    g.add_edge(0, 0, 2, {})
    g.add_edge(0, 0, 3, {})
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    assert degree_centrality(g).get_all() == {'0': 1.0, '1': 1.0, '2': 2 / 3, '3': 2 / 3}


def test_max_degree():
    from raphtory import Graph
    from raphtory.algorithms import max_degree
    g = Graph()
    g.add_edge(0, 0, 1, {})
    g.add_edge(0, 0, 2, {})
    g.add_edge(0, 0, 3, {})
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    assert max_degree(g) == 3
