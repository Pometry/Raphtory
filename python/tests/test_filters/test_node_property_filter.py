from raphtory import Graph, PersistentGraph, Prop
from raphtory import filter


def init_graph(graph):
    nodes = [
         (1, 1, {"p1": "shivam_kapoor", "p9": 5, "p10": "Paper_airplane"}, "fire_nation"),
         (2, 2, {"p1": "prop12", "p2": 2, "p10": "Paper_ship"}, "air_nomads"),
         (3, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 3, {"p2": 6, "p3": 1, "p10": "Paper_airplane"}, "fire_nation"),
         (4, 1, {"p1": "shivam_kapoor", "p9": 5}, "fire_nation"),
         (3, 4, {"p4": "pometry"}, None),
         (4, 4, {"p5": 12}, None),
    ]

    for time, id, props, node_type in nodes:
            graph.add_node(time, id, props, node_type)

    return graph


def test_filter_nodes_for_property_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") == 2
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2])
    assert result_ids == expected_ids

    filter_expr = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([1])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") != 2
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_lt():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") < 10
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2, 3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_le():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2, 3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_gt():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") > 2
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([3])
    assert result_ids == expected_ids


def test_nodes_for_property_ge():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2") >= 2
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2, 3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_in([6])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([3])
    assert result_ids == expected_ids

    filter_expr = filter.Property("p2").is_in([2, 6])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2, 3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_not_in([6])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_is_some():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_some()
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([2, 3])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_is_none():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p2").is_none()
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted([1, 4])
    assert result_ids == expected_ids


def test_filter_nodes_by_props_added_at_different_times():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr1 = filter.Property("p4") == "pometry"
    filter_expr2 = filter.Property("p5") == 12
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = sorted([4])
    assert result_ids == expected_ids


def test_filter_nodes_for_property_contains():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p10").contains("Paper")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 2, 3]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().any().contains("Paper")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 2, 3]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().latest().contains("Paper")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 2, 3]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").constant().contains("Paper")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = []
    assert result_ids == expected_ids

def test_filter_nodes_for_property_contains_not():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p10").contains_not("ship")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 3, 4]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().any().contains_not("ship")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 3]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().latest().contains_not("ship")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 3, 4]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").constant().contains_not("ship")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = [1, 2, 3, 4]
    assert result_ids == expected_ids

