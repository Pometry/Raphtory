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
            graph.add_node(time, str(id), props, node_type)

    return graph


def test_filter_nodes_for_node_name_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.name() == "3"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["3"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_name_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.name() != "2"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1", "3", "4"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_name_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.name().is_in(["1"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1"])
    assert result_ids == expected_ids

    filter_expr = filter.Node.name().is_in(["2", "3"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["2", "3"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_name_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.name().is_not_in(["1"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["2", "3", "4"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_eq():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type() == "fire_nation"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1", "3"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_ne():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type() != "fire_nation"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["2", "4"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().is_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1", "3"])
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().is_in(["fire_nation", "air_nomads"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1", "2", "3"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_not_in():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["2", "4"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_contains():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().contains("fire")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["1", "3"])
    assert result_ids == expected_ids


def test_filter_nodes_for_node_type_not_contains():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().not_contains("fire")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["2", "4"])
    assert result_ids == expected_ids


def test_filter_nodes_for_fuzzy_search():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, True)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, False)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().fuzzy_search("air_noma", 2, False)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2"]
    assert result_ids == expected_ids


def test_filter_nodes_for_not_node_type():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(~filter_expr).nodes.id)
    expected_ids = sorted(["1", "3"])
    assert result_ids == expected_ids

