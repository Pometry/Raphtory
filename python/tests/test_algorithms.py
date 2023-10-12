import pytest

def test_degree_centrality():
    from raphtory import Graph
    from raphtory.algorithms import degree_centrality

    g = Graph()
    g.add_edge(0, 1, 2, {})
    g.add_edge(0, 1, 3, {})
    g.add_edge(0, 1, 4, {})
    g.add_edge(0, 2, 3, {})
    g.add_edge(0, 2, 4, {})
    assert degree_centrality(g).get_with_names() == {
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
    assert res_one.get_with_names() == {"1": ["1"], "2": ["1", "2"], "3": None, "4": ["1", "4"]}
    assert (
        res_two.get_with_names()
        == {"1": ["1"], "2": ["1", "2"], "3": ["1", "2", "3"], "4": ["1", "4"]}
    ) or (
        res_two.get_with_names()
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

    