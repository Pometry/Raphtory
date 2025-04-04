from raphtory import Graph, PersistentGraph, Prop
from raphtory import filter


def init_graph(graph):
    nodes = [
         (1, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (2, 2, {"p1": "prop12", "p2": 2}, "air_nomads"),
         (3, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 3, {"p2": 6, "p3": 1}, "fire_nation"),
         (4, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 4, {"p4": "pometry"}, None),
         (4, 4, {"p5": 12}, None),
    ]

    for time, id, props, node_type in nodes:
            graph.add_node(time, str(id), props, node_type)

    return graph


def test_node_composite_filter():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr1 = filter.Property("p2") == 2
    filter_expr2 = filter.Property("p1") == "kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = sorted([])
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") == 2
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 | filter_expr2).nodes.id)
    expected_ids = sorted(["1", "2"])
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p9") == 5
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = sorted(["1"])
    assert result_ids == expected_ids

    filter_expr1 = filter.Node.node_type() == "fire_nation"
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = sorted(["1"])
    assert result_ids == expected_ids


    filter_expr1 = filter.Node.name() == "2"
    filter_expr2 = filter.Property("p2") == 2
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = sorted(["2"])
    assert result_ids == expected_ids


    filter_expr1 = filter.Node.name() == "2"
    filter_expr2 = filter.Property("p2") == 2
    filter_expr3 = filter.Property("p9") == 5
    result_ids = sorted(graph.filter_nodes((filter_expr1 & filter_expr2) | filter_expr3).nodes.id)
    expected_ids = sorted(["1", "2"])
    assert result_ids == expected_ids
