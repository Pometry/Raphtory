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


def test_edge_composite_filter():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr1 = filter.Property("p2") == 2
    filter_expr2 = filter.Property("p1") == "kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([])
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") > 2
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 | filter_expr2).edges.id)
    expected_ids = sorted([('1', '2'), ('2', '1'), ('3', '1')])
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") < 9
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([("1", "2")])
    assert result_ids == expected_ids

    filter_expr1 = filter.Edge.src().name() == "1"
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([("1", "2")])
    assert result_ids == expected_ids


    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([('2', '1'), ('3', '1')])
    assert result_ids == expected_ids


    filter_expr1 = filter.Edge.src().name() == "1"
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    filter_expr3 = filter.Property("p3") == 5
    result_ids = sorted(graph.filter_edges((filter_expr1 & filter_expr2) | filter_expr3).edges.id)
    expected_ids = sorted([("1", "2")])
    assert result_ids == expected_ids


def test_not_edge_composite_filter():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(~filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([('1', '2'), ('2', '3')])
    assert result_ids == expected_ids

    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(~(filter_expr1 & filter_expr2)).edges.id)
    expected_ids = sorted([('1', '2'), ('2', '3')])
    assert result_ids == expected_ids

