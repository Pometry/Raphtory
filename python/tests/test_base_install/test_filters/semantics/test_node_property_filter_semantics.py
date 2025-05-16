from raphtory import Graph, PersistentGraph, Prop
from raphtory import filter


def init_graph(graph):
    nodes = [
        (6, "N1", {"p1": 2}),
        (7, "N1", {"p1": 1}),
        (6, "N2", {"p1": 1}),
        (7, "N2", {"p1": 2}),
        (8, "N3", {"p1": 1}),
        (9, "N4", {"p1": 1}),
        (5, "N5", {"p1": 1}),
        (6, "N5", {"p1": 2}),
        (5, "N6", {"p1": 1}),
        (6, "N6", {"p1": 1}),
        (3, "N7", {"p1": 1}),
        (5, "N7", {"p1": 1}),
        (3, "N8", {"p1": 1}),
        (4, "N8", {"p1": 2}),
        (2, "N9", {"p1": 2}),
        (2, "N10", {"q1": 0}),
        (2, "N10", {"p1": 3}),
        (2, "N11", {"p1": 3}),
        (2, "N11", {"q1": 0}),
        (2, "N12", {"q1": 0}),
        (3, "N12", {"p1": 3}),
        (2, "N13", {"q1": 0}),
        (3, "N13", {"p1": 3}),
        (2, "N14", {"q1": 0}),
        (2, "N15", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p1": 1},
        "N4": {"p1": 2},
        "N9": {"p1": 1},
        "N10": {"p1": 1},
        "N11": {"p1": 1},
        "N12": {"p1": 1},
        "N13": {"p1": 1},
        "N14": {"p1": 1},
        "N15": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    return graph


def init_graph_for_secondary_indexes(graph):
    graph.add_node(1, "N16", {"p1": 2})
    graph.add_node(1, "N16", {"p1": 1})

    graph.add_node(1, "N17", {"p1": 1})
    graph.add_node(1, "N17", {"p1": 2})

    return graph


def test_constant_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").constant() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"])
    assert result_ids == expected_ids


def test_temporal_any_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").temporal().any() == 1

    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"])
    assert result_ids == expected_ids


def test_temporal_any_semantics_for_secondary_indexes():
    graph = Graph()
    graph = init_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().any() == 1

    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(
        ["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
    )
    assert result_ids == expected_ids


def test_temporal_latest_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_temporal_latest_semantics_for_secondary_indexes():
    graph = Graph()
    graph = init_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N16", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_property_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N14", "N15", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_property_semantics_for_secondary_indexes():
    graph = Graph()
    graph = init_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N14", "N15", "N16", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_property_semantics_only_constant():
    # For this graph there won't be any temporal property index for property name "p1".
    graph = Graph()
    nodes = [
        (2, "N1", {"q1": 0}),
        (2, "N2", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p1": 1},
        "N2": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N2"])
    assert result_ids == expected_ids


def test_property_semantics_only_temporal():
    # For this graph there won't be any constant property index for property name "p1".
    graph = Graph()
    nodes = [
        (1, "N1", {"p1": 1}),
        (2, "N2", {"p1": 1}),
        (3, "N2", {"p1": 2}),
        (2, "N3", {"p1": 2}),
        (3, "N3", {"p1": 1}),
        (2, "N4", {}),
    ]

    for time, label, props in nodes:
        graph.add_node(time, label, props)

    constant_properties = {
        "N1": {"p2": 1},
        "N2": {"p1": 1},
    }

    for label, props in constant_properties.items():
        graph.node(label).add_constant_properties(props)

    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N3"])
    assert result_ids == expected_ids
