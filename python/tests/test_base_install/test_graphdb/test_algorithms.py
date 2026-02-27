import pytest

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
    expected = {k: {"lcc": v} for k, v in expected.items()}
    actual = algorithms.local_clustering_coefficient_batch(g, list(range(1, 9)))
    # actual = {str(i): actual[i]["lcc"] for i in range(1, 9)}
    assert actual == expected


def test_connected_components():
    g = gen_graph()
    actual = algorithms.weakly_connected_components(g)
    c = actual[1]
    expected = {1: c, 2: c, 3: c, 4: c, 5: c, 6: c, 7: c, 8: c}
    assert actual == expected


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
    actual = {
        i: sorted(actual[i]["in_components"].id.collect()) for i in expected.keys()
    }
    assert actual == expected


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
    actual = {
        i: sorted(actual[i]["out_components"].id.collect()) for i in expected.keys()
    }
    assert actual == expected


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

    res_full_graph = {i: res_full_graph[i] for i in expected_full_graph.keys()}
    assert res_full_graph == expected_full_graph

    g_window = g.window(0, 2)
    res_window = algorithms.weakly_connected_components(g_window)
    expected_window = {1: res_window[1], 2: res_window[1]}
    assert {i: res_window[i] for i in expected_window.keys()} == expected_window

    g_window = g.window(2, 3)
    res_window = algorithms.weakly_connected_components(g_window)
    expected_window = {3: res_window[3], 4: res_window[3]}
    assert {i: res_window[i] for i in expected_window.keys()} == expected_window


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
    expected = {
        1: c1,
        2: c1,
        3: c1,
        4: c2,
        5: c2,
    }
    result = {i: res_zero_two[i] for i in expected.keys()}
    assert result == expected

    res_three_five = algorithms.weakly_connected_components(g_layer_three_five)
    c6 = res_three_five[6]
    c7 = res_three_five[8]
    expected = {
        6: c6,
        7: c6,
        8: c7,
        9: c7,
    }
    result = {i: res_three_five[i] for i in expected.keys()}
    assert result == expected


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
    expected = {1: c, 2: c}
    result = {i: res_zero_two[i] for i in expected.keys()}
    assert result == expected

    res_three_five = algorithms.weakly_connected_components(g_layer_three_five)
    c = res_three_five[8]
    expected = {8: c, 9: c}
    result = {i: res_three_five[i] for i in expected.keys()}
    assert result == expected


def test_algo_result():
    g = gen_graph()

    actual = algorithms.weakly_connected_components(g)
    c = actual[1]
    expected = {"1": c, "2": c, "3": c, "4": c, "5": c, "6": c, "7": c, "8": c}
    result = {i: actual[i] for i in expected.keys()}
    assert result == expected
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

    sorted_actual = sorted(list(actual.items()), key=lambda x: x[0])

    assert sorted_actual == expected_array

    # assert sorted(actual.top_k(8).items()) == expected_array
    # assert len(actual.groups()[0][1]) == 8
    # assert type(actual.to_df()) == pandas.core.frame.DataFrame
    # df = actual.sorted_by_id().to_df()
    # expected_result = pd.DataFrame({"node": list(range(1, 9)), "value": [c] * 8})
    # assert df.equals(expected_result)
    # Algo Str u64
    actual = algorithms.weakly_connected_components(g)
    c = actual[1]
    expected = {
        "1": c,
        "2": c,
        "3": c,
        "4": c,
        "5": c,
        "6": c,
        "7": c,
        "8": c,
    }
    result = {i: actual[i] for i in expected.keys()}
    assert result == expected
    # algo str f64
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
    result = {i: actual[i]["pagerank_score"] for i in expected.keys()}
    assert result == expected
    assert actual.get("Not a node") is None
    # assert len(actual.to_df()) == 8
    # algo str vector
    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    expected = {
        1: [(11, "start")],
        3: [],
        2: [(11, "1"), (11, "start"), (12, "1")],
        4: [(12, "2")],
        5: [(13, "2")],
        6: [],
        7: [],
        8: [],
    }
    result = {}
    for i in expected.keys():
        if actual[i]["reachable_nodes"] is None:
            result[i] = []
        else:
            result[i] = [tuple(v.values()) for v in actual[i]["reachable_nodes"]]
    assert result == expected
    # print(actual.to_df())


def test_page_rank():
    g = gen_graph()
    actual = algorithms.pagerank(g)
    print(actual.top_k({"pagerank_score": "desc"}, 5))
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
    result = {i: actual[i]["pagerank_score"] for i in expected.keys()}
    assert result == expected


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
    result = {}
    for i in expected.keys():
        if actual[i]["reachable_nodes"] is None:
            result[i] = []
        else:
            result[i] = [tuple(v.values()) for v in actual[i]["reachable_nodes"]]
    assert result == expected


def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality

    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(0, 1, 4, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 2, 4, {})
    actual = degree_centrality(g)
    expected = {
        "1": {"degree_centrality": 1.0},
        "2": {"degree_centrality": 1.0},
        "3": {"degree_centrality": 2 / 3},
        "4": {"degree_centrality": 2 / 3},
    }
    assert actual == expected


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
    expected = {
        "1": ["1"],
        "2": ["1", "2"],
        "4": ["1", "4"],
    }
    result = {i: res_one[i]["path"] for i in expected.keys()}
    assert result == expected
    expected = {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    result = {i: res_two[i]["path"] for i in expected.keys()}
    assert result == expected
    """
    assert (
        res_two == {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    ) or (
        res_two == {"1": ["1"], "3": ["1", "4", "3"], "2": ["1", "2"], "4": ["1", "4"]}
    )"""


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
    assert res_one.get("F")["distance"] == 8.0
    assert res_one.get("F")["path"].name == ["A", "C", "E", "F"]
    assert res_two.get("D")["distance"] == 5.0
    assert res_two.get("F")["distance"] == 6.0
    assert res_two.get("D")["path"].name == ["B", "C", "D"]
    assert res_two.get("F")["path"].name == ["B", "C", "E", "F"]

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
    expected = {
        "0": 0.0,
        "1": 1.0,
        "2": 4.0,
        "3": 1.0,
        "4": 0.0,
        "5": 0.0,
    }
    result = {i: res[i]["betweenness_centrality"] for i in expected.keys()}
    assert result == expected

    res = betweenness_centrality(g, normalized=True)
    expected = {"0": 0.0, "1": 0.05, "2": 0.2, "3": 0.05, "4": 0.0, "5": 0.0}
    result = {i: res[i]["betweenness_centrality"] for i in expected.keys()}
    assert result == expected


def test_hits_algorithm():
    g = graph_loader.lotr_graph()
    assert algorithms.hits(g).get("Aldor") == {
        "hub_score": 0.0035840950440615416,
        "auth_score": 0.007476256228983402,
    }


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
    actual = algorithms.balance(g, "value_dec", "both")
    print(actual)
    expected = {"1": -26.0, "2": 7.0, "3": 12.0, "4": 5.0, "5": 2.0}
    result = {i: actual[i]["balance"] for i in expected.keys()}
    assert result == expected

    actual = algorithms.balance(g, "value_dec", "in")
    expected = {"1": 6.0, "2": 12.0, "3": 15.0, "4": 20.0, "5": 2.0}
    result = {i: actual[i]["balance"] for i in expected.keys()}
    assert result == expected

    actual = algorithms.balance(g, "value_dec", "out")
    expected = {"1": -32.0, "2": -5.0, "3": -3.0, "4": -15.0, "5": 0.0}
    result = {i: actual[i]["balance"] for i in expected.keys()}
    assert result == expected


def test_label_propagation_algorithm():
    g = Graph()
    edges_str = [
        (1, "R1", "R2"),
        (1, "R1", "R3"),
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
    result_node = algorithms.label_propagation(g, 10, seed)
    result = []
    print(result_node)
    for community_id in (2, 8):
        community = set()
        for node_id, value in result_node.items():
            if value["community_id"] == community_id:
                community.add(node_id.name)
        result.append(community)
    expected = [{"R2", "R3", "R1"}, {"G", "B4", "B3", "B2", "B1", "B5"}]
    assert result == expected


def test_k_core():
    graph = Graph()

    edges = [
        (1, 2, 1),
        (1, 3, 2),
        (1, 4, 3),
        (3, 1, 4),
        (3, 4, 5),
        (3, 5, 6),
        (4, 5, 7),
        (5, 6, 8),
        (5, 8, 9),
        (7, 5, 10),
        (8, 5, 11),
        (1, 9, 12),
        (9, 1, 13),
        (6, 3, 14),
        (4, 8, 15),
        (8, 3, 16),
        (5, 10, 17),
        (10, 5, 18),
        (10, 8, 19),
        (1, 11, 20),
        (11, 1, 21),
        (9, 11, 22),
        (11, 9, 23),
    ]

    for src, dst, ts in edges:
        graph.add_edge(ts, src, dst)

    result = algorithms.k_core(graph, 2, 100)
    result = sorted([node.id for node in result])
    result = [str(n_id) for n_id in result]
    actual = ["1", "3", "4", "5", "6", "8", "9", "10", "11"]

    assert result == actual


def test_temporal_SEIR():
    g = Graph()
    g.add_edge(1, 1, 2)
    g.add_edge(2, 2, 3)
    g.add_edge(3, 3, 4)
    g.add_edge(4, 4, 5)
    # Should be seeded with 2 vertices
    res = algorithms.temporal_SEIR(g, 2, 1.0, 0, rng_seed=1)

    seeded = [v for v in res.values() if v["infected"] == 0]
    assert len(seeded) == 2

    res = algorithms.temporal_SEIR(g, [1], 1.0, 0, rng_seed=1)
    sorted_actual = sorted(list(res.items()), key=lambda x: x[0])
    for i, (n, v) in enumerate(sorted_actual):
        assert n == g.node(i + 1)
        assert v["infected"] == i


##########
def test_nodestate_merge_test():
    from raphtory.algorithms import degree_centrality, pagerank

    g = Graph()

    """
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(0, 1, 4, {}) 
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 2, 4, {})
    """
    import gc
    import time

    N = 1_000_000

    print("start graph gen")
    now = time.time()
    add = g.add_edge
    gc.disable()
    try:
        for i in range(N):
            add(0, i, i + 1)
    finally:
        gc.enable()

    print(f"finished graph gen in: {time.time() - now}")
    algo_time = time.time()
    now = time.time()
    sg = g.subgraph(list(range(1, 200)))
    r1 = degree_centrality(g)
    print(f"finished degree centrality in: {time.time() - now}")
    now = time.time()
    r2 = pagerank(sg)
    print(f"finished pagerank in: {time.time() - now}")
    print(f"finished algos in: {time.time() - algo_time}")
    now = time.time()

    print()
    print(
        len(
            r2.merge(
                r1,
                "left",
                "left",
                {"pagerank_score": "left", "degree_centrality": "right"},
            )
        )
    )
    print(f"finished right merge in: {time.time() - now}")
    now = time.time()
    print(len(r1.merge(r2, "left", "left", {"pagerank_score": "right"})))
    print(f"finished left merge in: {time.time() - now}")


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
        1: [
            1.6817928305074292,
            0.4204482076268573,
            -0.4204482076268573,
            0.0,
            0.0,
            2.1022410381342866,
            0.4204482076268573,
            0.4204482076268573,
            2.1022410381342866,
            -0.8408964152537146,
            0.0,
            1.6817928305074292,
            0.0,
            -1.6817928305074292,
            0.0,
            -0.8408964152537146,
        ],
        2: [
            0.4204482076268573,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            0.0,
            0.8408964152537146,
            1.6817928305074292,
            1.6817928305074292,
            2.1022410381342866,
            -2.1022410381342866,
            0.0,
            0.4204482076268573,
            0.0,
            -0.4204482076268573,
            0.0,
            -2.1022410381342866,
        ],
        3: [
            0.4204482076268573,
            0.4204482076268573,
            -0.4204482076268573,
            0.0,
            0.0,
            2.1022410381342866,
            0.4204482076268573,
            0.4204482076268573,
            0.8408964152537146,
            -2.1022410381342866,
            0.0,
            0.4204482076268573,
            0.0,
            -0.4204482076268573,
            0.0,
            -2.1022410381342866,
        ],
        4: [
            -1.4014940254228576,
            0.560597610169143,
            1.121195220338286,
            -0.2802988050845715,
            0.2802988050845715,
            -0.2802988050845715,
            0.2802988050845715,
            0.0,
            -1.6817928305074292,
            0.0,
            0.0,
            -0.2802988050845715,
            0.2802988050845715,
            0.2802988050845715,
            -0.2802988050845715,
            -1.6817928305074292,
        ],
        5: [
            0.0,
            1.9620916355920008,
            -1.6817928305074292,
            -1.6817928305074292,
            0.2802988050845715,
            -0.2802988050845715,
            0.2802988050845715,
            1.4014940254228576,
            -0.2802988050845715,
            0.0,
            0.0,
            -1.6817928305074292,
            0.2802988050845715,
            0.2802988050845715,
            -0.2802988050845715,
            1.121195220338286,
        ],
        6: [
            -0.21022410381342865,
            0.6306723114402859,
            -1.6817928305074292,
            -1.4715687266940005,
            1.6817928305074292,
            -1.6817928305074292,
            0.0,
            -1.4715687266940005,
            -0.21022410381342865,
            0.0,
            0.0,
            -0.4204482076268573,
            1.6817928305074292,
            0.21022410381342865,
            -0.21022410381342865,
            -0.21022410381342865,
        ],
        7: [
            1.4014940254228576,
            1.9620916355920008,
            -0.2802988050845715,
            1.121195220338286,
            0.2802988050845715,
            -0.2802988050845715,
            1.6817928305074292,
            0.0,
            -0.2802988050845715,
            0.0,
            0.0,
            -0.2802988050845715,
            0.2802988050845715,
            1.6817928305074292,
            -1.6817928305074292,
            -1.6817928305074292,
        ],
        8: [
            -1.6817928305074292,
            1.6817928305074292,
            -0.8408964152537146,
            0.8408964152537146,
            0.8408964152537146,
            -0.8408964152537146,
            -1.6817928305074292,
            -0.8408964152537146,
            0.0,
            0.0,
            0.0,
            -1.6817928305074292,
            0.8408964152537146,
            0.0,
            0.0,
            0.0,
        ],
    }

    result = {n.id: v["embedding_state"] for n, v in result.items()}
    assert result == baseline
