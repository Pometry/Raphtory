def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality

    g = Graph()
    g.add_edge(0, 0, 1, {})
    g.add_edge(0, 0, 2, {})
    g.add_edge(0, 0, 3, {})
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    assert degree_centrality(g).get_all() == {
        "0": 1.0,
        "1": 1.0,
        "2": 2 / 3,
        "3": 2 / 3,
    }


def test_max_min_degree():
    from raphtory import Graph
    from raphtory.algorithms import max_degree
    from raphtory.algorithms import min_degree

    g = Graph()
    g.add_edge(0, 0, 1, {})
    g.add_edge(0, 0, 2, {})
    g.add_edge(0, 0, 3, {})
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    assert max_degree(g) == 3
    assert min_degree(g) == 2


def test_single_source_shortest_path():
    from raphtory import Graph
    from raphtory.algorithms import single_source_shortest_path

    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 3, 4, {})
    g.add_edge(0, 1, 4, {})
    g.add_edge(0, 2, 4, {})
    res_one = single_source_shortest_path(g, 1, 1)
    res_two = single_source_shortest_path(g, 1, 2)
    assert res_one.get_all() == {"1": ["1"], "2": ["1", "2"], "4": ["1", "4"]}
    assert (
        res_two.get_all()
        == {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    ) or (
        res_two.get_all()
        == {"1": ["1"], "3": ["1", "4", "3"], "2": ["1", "2"], "4": ["1", "4"]}
    )
