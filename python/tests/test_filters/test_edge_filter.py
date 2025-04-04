from raphtory import Graph, PersistentGraph, Prop
from raphtory import filter


def init_graph(graph):
    edge_data = [
        (1, "1", "2", {"p1": "shivam_kapoor"}, "fire_nation"),
        (2, "1", "2", {"p1": "shivam_kapoor", "p2": 4}, "fire_nation"),
        (2, "2", "3", {"p1": "prop12", "p2": 2}, "air_nomads"),
        (3, "3", "1", {"p2": 6, "p3": 1}, "fire_nation"),
        (3, "2", "1", {"p2": 6, "p3": 1}, None),
    ]

    for time, src, dst, props, edge_type in edge_data:
        graph.add_edge(time, src, dst, props, edge_type)

    return graph


def test_filter_edges_for_src_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.src() == "2"
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3")])
    assert result_ids == expected_ids


def test_filter_edges_for_src_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.src() != "1"
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_src_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.src().includes(["1"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2")])
    assert result_ids == expected_ids

    filter_expr = filter.Edge.src().includes(["1", "2"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3")])
    assert result_ids == expected_ids


def test_filter_edges_for_src_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.src().excludes(["1"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_dst_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.dst() == "1"
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_dst_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.dst() != "2"
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_dst_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.dst().includes(["2"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2")])
    assert result_ids == expected_ids

    filter_expr = filter.Edge.dst().includes(["2", "3"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "3")])
    assert result_ids == expected_ids


def test_filter_edges_for_dst_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Edge.dst().excludes(["1"])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "3")])
    assert result_ids == expected_ids


def test_edge_for_src_dst():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr1 = filter.Edge.src() == "3"
    filter_expr2 = filter.Edge.dst() == "1"
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([("3", "1")])
    assert result_ids == expected_ids
