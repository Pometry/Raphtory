import pytest
import pandas as pd
import pandas.core.frame

from raphtory import Graph
from raphtory import algorithms
from raphtory import graph_loader


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
    g.add_edge(11, 4, 7, {})
    g.add_edge(10, 5, 8, {})
    return g


def test_local_clustering_coefficient():
    g = gen_graph()
    expected = {
        "1": 0.0,
        "2": 0.3333333333333333,
        "3": 0.0,
        "4": 0.16666666666666666,
        "5": 0.3333333333333333,
        "6": 0.0,
        "7": 0.0,
        "8": 0.0,
    }
    actual = {
        str(i): algorithms.local_clustering_coefficient(g, g.node(i))
        for i in range(1, 9)
    }
    assert actual == expected
    actual = algorithms.local_clustering_coefficient_batch(g, list(range(1, 9)))
    actual = {str(i): actual[i] for i in range(1, 9)}
    assert actual == expected


def test_connected_components():
    g = gen_graph()
    actual = algorithms.weakly_connected_components(g)
    expected = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0, "7": 0, "8": 0}
    assert actual == expected
    assert actual["1"] == 0


def test_largest_connected_component():
    g = gen_graph()
    actual = g.largest_connected_component()
    expected = ["1", "2", "3", "4", "5", "6", "7", "8"]
    for node in expected:
        assert actual.has_node(node)


def test_in_components():
    g = gen_graph()
    actual = algorithms.in_components(g)
    expected = {
        1: [],
        2: [1],
        3: [1],
        4: [1, 2, 5],
        5: [1, 2, 5],
        6: [1, 2, 4, 5],
        7: [1, 2, 4, 5],
        8: [1, 2, 5],
    }
    assert len(actual) == len(expected)
    for k, v in expected.items():
        assert actual[k].id.sorted() == v


def test_in_component():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    g.add_edge(5, 3, 6)
    g.add_edge(6, 7, 3)

    actual = algorithms.in_component(g.node(3))
    correct = [g.node(7), g.node(1)]
    assert set(actual.nodes()) == set(correct)
    actual = algorithms.in_component(g.node(3).window(1, 6))
    correct = [g.node(1)]
    assert set(actual.nodes()) == set(correct)


def test_out_components():
    g = gen_graph()
    actual = algorithms.out_components(g)
    expected = {
        "1": [2, 3, 4, 5, 6, 7, 8],
        "2": [4, 5, 6, 7, 8],
        "3": [],
        "4": [6, 7],
        "5": [4, 5, 6, 7, 8],
        "6": [],
        "7": [],
        "8": [],
    }
    assert len(actual) == len(expected)
    for k, v in expected.items():
        assert actual[k].id.sorted() == v


def test_out_component():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 1, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    g.add_edge(5, 3, 6)
    g.add_edge(6, 7, 3)

    actual = algorithms.out_component(g.node(3))
    correct = [g.node(4), g.node(5), g.node(6)]
    assert set(actual.nodes()) == set(correct)
    actual = algorithms.out_component(g.node(4).at(4))
    correct = [g.node(5)]
    assert set(actual.nodes()) == set(correct)


def test_empty_algo():
    g = Graph()
    assert algorithms.weakly_connected_components(g) == {}
    assert algorithms.pagerank(g, 20) == {}


def test_algo_result_windowed_graph():
    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 3, 4, {})
    g.add_edge(3, 5, 6, {})
    g.add_edge(10, 10, 11, {})

    res_full_graph = algorithms.weakly_connected_components(g)
    c1 = res_full_graph[1]
    c3 = res_full_graph[3]
    c5 = res_full_graph[5]
    c10 = res_full_graph[10]
    expected_full_graph = {
        1: c1,
        2: c1,
        3: c3,
        4: c3,
        5: c5,
        6: c5,
        10: c10,
        11: c10,
    }

    assert res_full_graph == expected_full_graph

    g_window = g.window(0, 2)
    res_window = algorithms.weakly_connected_components(g_window)
    assert res_window == {1: res_window[1], 2: res_window[1]}

    g_window = g.window(2, 3)
    res_window = algorithms.weakly_connected_components(g_window)
    assert res_window == {3: res_window[3], 4: res_window[3]}


def test_algo_result_layered_graph():
    g = Graph()
    g.add_edge(0, 1, 2, {}, layer="ZERO-TWO")
    g.add_edge(1, 1, 3, {}, layer="ZERO-TWO")
    g.add_edge(2, 4, 5, {}, layer="ZERO-TWO")
    g.add_edge(3, 6, 7, {}, layer="THREE-FIVE")
    g.add_edge(4, 8, 9, {}, layer="THREE-FIVE")

    g_layer_zero_two = g.layer("ZERO-TWO")
    g_layer_three_five = g.layer("THREE-FIVE")

    res_zero_two = algorithms.weakly_connected_components(g_layer_zero_two)
    c1 = res_zero_two[1]
    c2 = res_zero_two[4]

    assert res_zero_two == {
        1: c1,
        2: c1,
        3: c1,
        4: c2,
        5: c2,
    }

    res_three_five = algorithms.weakly_connected_components(g_layer_three_five)
    c6 = res_three_five[6]
    c7 = res_three_five[8]
    assert res_three_five == {
        6: c6,
        7: c6,
        8: c7,
        9: c7,
    }


def test_algo_result_window_and_layered_graph():
    g = Graph()
    g.add_edge(0, 1, 2, {}, layer="ZERO-TWO")
    g.add_edge(1, 1, 3, {}, layer="ZERO-TWO")
    g.add_edge(2, 4, 5, {}, layer="ZERO-TWO")
    g.add_edge(3, 6, 7, {}, layer="THREE-FIVE")
    g.add_edge(4, 8, 9, {}, layer="THREE-FIVE")

    g_layer_zero_two = g.window(0, 1).layer("ZERO-TWO")
    g_layer_three_five = g.window(4, 5).layer("THREE-FIVE")

    res_zero_two = algorithms.weakly_connected_components(g_layer_zero_two)
    c = res_zero_two[1]
    assert res_zero_two == {1: c, 2: c}

    res_three_five = algorithms.weakly_connected_components(g_layer_three_five)
    c = res_three_five[8]
    assert res_three_five == {8: c, 9: c}


def test_algo_result():
    g = gen_graph()

    actual = algorithms.weakly_connected_components(g)
    c = actual[1]
    expected = {"1": c, "2": c, "3": c, "4": c, "5": c, "6": c, "7": c, "8": c}
    assert actual == expected
    assert actual.get("not a node") is None
    expected_array = [
        (g.node("1"), c),
        (g.node("2"), c),
        (g.node("3"), c),
        (g.node("4"), c),
        (g.node("5"), c),
        (g.node("6"), c),
        (g.node("7"), c),
        (g.node("8"), c),
    ]
    assert list(actual.sorted_by_id().items()) == expected_array
    assert sorted(actual.top_k(8).items()) == expected_array
    assert len(actual.groups()[0][1]) == 8
    assert type(actual.to_df()) == pandas.core.frame.DataFrame
    df = actual.sorted_by_id().to_df()
    expected_result = pd.DataFrame({"node": list(range(1, 9)), "value": [c] * 8})
    assert df.equals(expected_result)
    # Algo Str u64
    actual = algorithms.weakly_connected_components(g)
    c = actual[1]
    assert actual == {
        "1": c,
        "2": c,
        "3": c,
        "4": c,
        "5": c,
        "6": c,
        "7": c,
        "8": c,
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
    assert actual == expected_result
    assert actual.get("Not a node") is None
    assert len(actual.to_df()) == 8
    # algo str vector
    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    assert actual == {
        1: [(11, "start")],
        3: [],
        2: [(11, "1"), (11, "start"), (12, "1")],
        4: [(12, "2")],
        5: [(13, "2")],
        6: [],
        7: [],
        8: [],
    }
    print(actual.to_df())


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
    assert actual == expected


def test_temporal_reachability():
    g = gen_graph()

    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    expected = {
        "1": [(11, "start")],
        "2": [
            (11, "1"),
            (11, "start"),
            (12, "1"),
        ],
        "3": [],
        "4": [(12, "2")],
        "5": [(13, "2")],
        "6": [],
        "7": [],
        "8": [],
    }

    assert actual == expected


def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality

    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(0, 1, 4, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 2, 4, {})
    assert degree_centrality(g) == {
        "1": 1.0,
        "2": 1.0,
        "3": 2 / 3,
        "4": 2 / 3,
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
    assert res_one == {
        "1": ["1"],
        "2": ["1", "2"],
        "4": ["1", "4"],
    }
    assert (
        res_two == {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    ) or (
        res_two == {"1": ["1"], "3": ["1", "4", "3"], "2": ["1", "2"], "4": ["1", "4"]}
    )


def test_dijsktra_shortest_paths():
    from raphtory import Graph
    from raphtory.algorithms import dijkstra_single_source_shortest_paths

    g = Graph()
    g.add_edge(0, "A", "B", {"weight": 4.0})
    g.add_edge(1, "A", "C", {"weight": 4.0})
    g.add_edge(2, "B", "C", {"weight": 2.0})
    g.add_edge(3, "C", "D", {"weight": 3.0})
    g.add_edge(4, "C", "E", {"weight": 1.0})
    g.add_edge(5, "C", "F", {"weight": 6.0})
    g.add_edge(6, "D", "F", {"weight": 2.0})
    g.add_edge(7, "E", "F", {"weight": 3.0})
    res_one = dijkstra_single_source_shortest_paths(g, "A", ["F"])
    res_two = dijkstra_single_source_shortest_paths(g, "B", ["D", "E", "F"])
    assert res_one.get("F")[0] == 8.0
    assert res_one.get("F")[1].name == ["A", "C", "E", "F"]
    assert res_two.get("D")[0] == 5.0
    assert res_two.get("F")[0] == 6.0
    assert res_two.get("D")[1].name == ["B", "C", "D"]
    assert res_two.get("F")[1].name == ["B", "C", "E", "F"]

    with pytest.raises(Exception) as excinfo:
        dijkstra_single_source_shortest_paths(g, "HH", ["F"])
    assert "Node HH does not exist" in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        dijkstra_single_source_shortest_paths(g, "A", ["F"], weight="NO")
    assert "Property NO does not exist" in str(excinfo.value)


def test_betweenness_centrality():
    from raphtory import Graph
    from raphtory.algorithms import betweenness_centrality

    g = Graph()
    edges = [
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 3),
        (1, 4),
        (2, 3),
        (2, 4),
        (2, 5),
        (3, 2),
        (3, 1),
        (3, 3),
    ]
    for e in edges:
        g.add_edge(0, e[0], e[1], {})

    res = betweenness_centrality(g, normalized=False)
    assert res == {
        "0": 0.0,
        "1": 1.0,
        "2": 4.0,
        "3": 1.0,
        "4": 0.0,
        "5": 0.0,
    }

    res = betweenness_centrality(g, normalized=True)
    assert res == {
        "0": 0.0,
        "1": 0.05,
        "2": 0.2,
        "3": 0.05,
        "4": 0.0,
        "5": 0.0,
    }


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
    result = algorithms.balance(g, "value_dec", "both")
    assert result == {"1": -26.0, "2": 7.0, "3": 12.0, "4": 5.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", "in")
    assert result == {"1": 6.0, "2": 12.0, "3": 15.0, "4": 20.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", "out")
    assert result == {"1": -32.0, "2": -5.0, "3": -3.0, "4": -15.0, "5": 0.0}


def test_label_propagation_algorithm():
    g = Graph()
    edges_str = [
        (1, "R1", "R2"),
        (1, "R2", "R3"),
        (1, "R3", "G"),
        (1, "G", "B1"),
        (1, "G", "B3"),
        (1, "B1", "B2"),
        (1, "B2", "B3"),
        (1, "B2", "B4"),
        (1, "B3", "B4"),
        (1, "B3", "B5"),
        (1, "B4", "B5"),
    ]
    for time, src, dst in edges_str:
        g.add_edge(time, src, dst)
    seed = [5] * 32
    result_node = algorithms.label_propagation(g, seed)
    result = []
    for group in result_node:
        result.append({n.name for n in group})
    expected = [{"R2", "R3", "R1"}, {"G", "B4", "B3", "B2", "B1", "B5"}]
    assert len(result) == len(expected)
    for group in expected:
        assert group in result


def test_temporal_SEIR():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 2, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    # Should be seeded with 2 vertices
    res = algorithms.temporal_SEIR(g, 2, 1.0, 0, rng_seed=1)
    seeded = [v for v in res.values() if v.infected == 0]
    assert len(seeded) == 2

    res = algorithms.temporal_SEIR(g, [1], 1.0, 0, rng_seed=1).sorted(reverse=False)
    for i, (n, v) in enumerate(res.items()):
        assert n == g.node(i + 1)
        assert v.infected == i


def test_max_weight_matching():
    g = Graph()
    g.add_edge(0, 1, 2, {"weight": 5})
    g.add_edge(0, 2, 3, {"weight": 11})
    g.add_edge(0, 3, 4, {"weight": 5})

    # Run max weight matching with max cardinality set to false
    max_weight = algorithms.max_weight_matching(g, "weight", False)
    max_cardinality = algorithms.max_weight_matching(g, "weight")

    assert len(max_weight) == 1
    assert len(max_cardinality) == 2
    assert (2, 3) in max_weight
    assert (1, 2) in max_cardinality
    assert (3, 4) in max_cardinality

    assert max_weight.edges().id == [(2, 3)]
    assert sorted(max_cardinality.edges().id) == [(1, 2), (3, 4)]

    assert max_weight.src(3).id == 2
    assert max_weight.src(2) is None

    assert max_weight.dst(2).id == 3
    assert max_weight.dst(3) is None


def test_fast_rp():
    g = Graph()
    edges = [
        (1, 2, 1),
        (1, 3, 1),
        (2, 3, 1),
        (4, 5, 1),
        (4, 6, 1),
        (4, 7, 1),
        (5, 6, 1),
        (5, 7, 1),
        (6, 7, 1),
        (6, 8, 1),
    ]
    for src, dst, ts in edges:
        g.add_edge(ts, src, dst)

    result = algorithms.fast_rp(g, 16, 1.0, [1.0, 1.0], 42)
    baseline = {
        "7": [
            0.0,
            3.3635856610148585,
            -1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            0.0,
            -1.6817928305074292,
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
        ],
        "6": [
            -1.6817928305074292,
            5.045378491522287,
            -1.6817928305074292,
            0.0,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            -1.6817928305074292,
            0.0,
            0.0,
            -3.3635856610148585,
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
        ],
        "5": [
            0.0,
            3.3635856610148585,
            -1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            0.0,
            -1.6817928305074292,
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
        ],
        "2": [
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            3.3635856610148585,
            1.6817928305074292,
            1.6817928305074292,
            3.3635856610148585,
            -3.3635856610148585,
            0.0,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            -3.3635856610148585,
        ],
        "8": [
            -1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            0.0,
            -1.6817928305074292,
            1.6817928305074292,
            0.0,
            0.0,
            0.0,
        ],
        "3": [
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            3.3635856610148585,
            1.6817928305074292,
            1.6817928305074292,
            3.3635856610148585,
            -3.3635856610148585,
            0.0,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            -3.3635856610148585,
        ],
        "1": [
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            3.3635856610148585,
            1.6817928305074292,
            1.6817928305074292,
            3.3635856610148585,
            -3.3635856610148585,
            0.0,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            -3.3635856610148585,
        ],
        "4": [
            0.0,
            3.3635856610148585,
            -1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            0.0,
            -1.6817928305074292,
            1.6817928305074292,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
        ],
    }

    assert result == baseline
