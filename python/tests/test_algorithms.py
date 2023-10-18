import pytest
import pandas as pd
import pandas.core.frame
from raphtory import Graph, GraphWithDeletions, PyDirection
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
    g.add_edge(10, 5, 8, {})
    return g


def test_connected_components():
    g = gen_graph()
    actual = algorithms.weakly_connected_components(g, 20)
    expected = {"1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1, "7": 1, "8": 1}
    assert actual.get_all_with_names() == expected
    assert actual.get("1") == 1


def test_empty_algo():
    g = Graph()
    assert algorithms.weakly_connected_components(g, 20).get_all_with_names() == {}
    assert algorithms.pagerank(g, 20).get_all_with_names() == {}


def test_algo_result_windowed_graph():
    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(1, 1, 2, {})
    g.add_edge(2, 3, 4, {})
    g.add_edge(3, 5, 6, {})
    g.add_edge(10, 10, 11, {})

    res_full_graph = algorithms.weakly_connected_components(g, 20)
    assert sorted(res_full_graph.get_all_with_names().items()) == [('1', 1), ('10', 10), ('11', 10), ('2', 1), ('3', 3), ('4', 3), ('5', 5), ('6', 5)]

    g_window = g.window(0, 2)
    res_window = algorithms.weakly_connected_components(g_window, 20) 
    assert sorted(res_window.get_all_with_names().items()) == [('1', 1), ('2', 1)]

    g_window = g.window(2, 3)
    res_window = algorithms.weakly_connected_components(g_window, 20) 
    assert sorted(res_window.get_all_with_names().items()) == [('3', 3), ('4', 3)]


# def test_algo_result_layered_graph():
#     g = Graph()
#     g.add_edge(0, 1, 2, {}, layer="ZERO-TWO")
#     g.add_edge(1, 1, 3, {}, layer="ZERO-TWO")
#     g.add_edge(2, 4, 5, {}, layer="ZERO-TWO")
#     g.add_edge(3, 6, 7, {}, layer="THREE-FIVE")
#     g.add_edge(4, 8, 9, {}, layer="THREE-FIVE")

#     g_layer_zero_two = g.layer("ZERO-TWO")
#     g_layer_three_five = g.layer("THREE-FIVE")

#     res_zero_two = algorithms.weakly_connected_components(g_layer_zero_two, 20) 
#     assert sorted(res_zero_two.get_all_with_names().items()) == [('1', 1), ('2', 1), ('4', 4), ('5', 4)]

#     res_three_five = algorithms.weakly_connected_components(g_layer_three_five, 20) 
#     assert sorted(res_three_five.get_all_with_names().items()) == [('3', 3), ('4', 3)]


def test_algo_result():
    g = gen_graph()

    actual = algorithms.weakly_connected_components(g, 20)
    expected = {"1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1, "7": 1, "8": 1}
    assert actual.get_all_with_names() == expected
    assert actual.get("1") == 1
    assert actual.get("not a node") == None
    expected_array = [
        (g.vertex("1"), 1),
        (g.vertex("2"), 1),
        (g.vertex("3"), 1),
        (g.vertex("4"), 1),
        (g.vertex("5"), 1),
        (g.vertex("6"), 1),
        (g.vertex("7"), 1),
        (g.vertex("8"), 1),
    ]
    assert actual.sort_by_vertex_name(False) == expected_array
    assert sorted(actual.top_k(8)) == expected_array
    assert len(actual.group_by()[1]) == 8
    assert type(actual.to_df()) == pandas.core.frame.DataFrame
    df = actual.to_df()
    expected_result = pd.DataFrame({"Key": [1], "Value": [1]})
    row_with_one = df[df["Key"] == 1]
    row_with_one.reset_index(inplace=True, drop=True)
    print(row_with_one)
    assert row_with_one.equals(expected_result)
    # Algo Str u64
    actual = algorithms.weakly_connected_components(g)
    all_res = actual.get_all_with_names()
    sorted_res = {k: all_res[k] for k in sorted(all_res)}
    assert sorted_res == {
        "1": 1,
        "2": 1,
        "3": 1,
        "4": 1,
        "5": 1,
        "6": 1,
        "7": 1,
        "8": 1,
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
    assert actual.get_all_with_names() == expected_result
    assert actual.get("Not a node") == None
    assert len(actual.to_df()) == 8
    # algo str vector
    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    assert sorted(actual.get_all_with_names()) == ["1", "2", "3", "4", "5", "6", "7", "8"]


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
    assert actual.get_all_with_names() == expected


def test_temporal_reachability():
    g = gen_graph()

    actual = algorithms.temporally_reachable_nodes(g, 20, 11, [1, 2], [4, 5])
    expected = {
        "1": [(11, "start")],
        "2": [(11, "start"), (12, "1"), (11, "1")],
        "3": [],
        "4": [(12, "2")],
        "5": [(13, "2")],
        "6": [],
        "7": [],
        "8": [],
    }

    assert actual.get_all_with_names() == expected


def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality

    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(0, 1, 4, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 2, 4, {})
    assert degree_centrality(g).get_all_with_names() == {
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
    assert res_one.get_all_with_names() == {"1": ["1"], "2": ["1", "2"], "3": None, "4": ["1", "4"]}
    assert (
        res_two.get_all_with_names()
        == {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    ) or (
        res_two.get_all_with_names()
        == {"1": ["1"], "3": ["1", "4", "3"], "2": ["1", "2"], "4": ["1", "4"]}
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
    assert res_one.get("F")[1] == ["A", "C", "E", "F"]
    assert res_two.get("D")[0] == 5.0
    assert res_two.get("F")[0] == 6.0
    assert res_two.get("D")[1] == ["B", "C", "D"]
    assert res_two.get("F")[1] == ["B", "C", "E", "F"]

    with pytest.raises(ValueError) as excinfo:
        dijkstra_single_source_shortest_paths(g, "HH", ["F"])
    assert "Source vertex not found" in str(excinfo.value)

    with pytest.raises(ValueError) as excinfo:
        dijkstra_single_source_shortest_paths(g, "A", ["F"], weight="NO")
    assert "Weight property not found on edges" in str(excinfo.value)

    
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
        (3, 3)
    ]
    for e in edges:
        g.add_edge(0, e[0], e[1], {})

    res = betweenness_centrality(g, normalized=False)
    assert res.get_all_with_names() == { "0": 0.0, '1': 1.0, "2": 4.0, "3": 1.0, "4": 0.0, "5": 0.0 }

    res = betweenness_centrality(g, normalized=True)
    assert res.get_all_with_names() == { "0": 0.0, '1': 0.05, "2": 0.2, "3": 0.05, "4": 0.0, "5": 0.0}


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
    result = algorithms.balance(g, "value_dec", PyDirection("BOTH"), None).get_all_with_names()
    assert result == {"1": -26.0, "2": 7.0, "3": 12.0, "4": 5.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", PyDirection("IN"), None).get_all_with_names()
    assert result == {"1": 6.0, "2": 12.0, "3": 15.0, "4": 20.0, "5": 2.0}

    result = algorithms.balance(g, "value_dec", PyDirection("OUT"), None).get_all_with_names()
    assert result == {"1": -32.0, "2": -5.0, "3": -3.0, "4": -15.0, "5": 0.0}
