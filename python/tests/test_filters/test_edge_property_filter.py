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


def test_filter_edges_for_property_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") == 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "3")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") != 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_lt():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") < 10
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_le():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_gt():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") > 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1")])
    assert result_ids == expected_ids


def test_edges_for_property_ge():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") >= 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").includes([6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("3", "1")])
    assert result_ids == expected_ids

    filter_expr = filter.Property("p2").includes([2, 6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").excludes([6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "3")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_is_some():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_some()
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1")])
    assert result_ids == expected_ids


def test_filter_edges_for_property_is_none():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_none()
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted(["1"])
    assert result_ids == expected_ids
