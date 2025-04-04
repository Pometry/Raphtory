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


def test_temporal_any_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").temporal().any() == 1

    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"])
    assert result_ids == expected_ids


def test_temporal_latest_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_temporal_any_semantics_for_secondary_indexes():
    graph = Graph()
    graph = init_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().any() == 1

    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"])
    assert result_ids == expected_ids


def test_temporal_latest_semantics_for_secondary_indexes():
    graph = Graph()
    graph = init_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N16", "N3", "N4", "N6", "N7"])
    assert result_ids == expected_ids


def test_property_semantics_for_secondary_indexes():
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


def test_constant_semantics():
    graph = Graph()
    graph = init_graph(graph)

    filter_expr = filter.Property("p1").constant() == 1
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = sorted(["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"])
    assert result_ids == expected_ids


def test_property_constant_semantics():
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


def test_property_temporal_semantics():
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


#

def build_graph():
    graph = Graph()
    graph.add_edge(0, 1, 2, {"test_str": "first", "test_int": 0})
    graph.add_edge(1, 2, 3, {"test_str": "second", "test_int": 1})
    graph.add_edge(2, 3, 4, {"test_int": 2})
    graph.add_edge(3, 3, 4, {"test_int": 3})
    graph.add_edge(4, 2, 3, {"test_bool": True})
    graph.add_edge(5, 2, 3, {"test_str": "third"})

    graph.add_node(0, 1, {"node_str": "first", "node_int": 1})
    graph.add_node(1, 1, {"node_str": "second", "node_int": 2})
    graph.add_node(1, 2, {"node_str": "second", "node_int": 2})
    graph.add_node(2, 3, {"node_str": "third", "node_int": 3})
    graph.add_node(3, 4, {"node_str": "fourth", "node_int": 4, "node_bool": True})

    graph.node(1).add_constant_properties({"c_prop1": "fire_nation"})
    graph.node(2).add_constant_properties({"c_prop1": "water_tribe"})
    graph.node(3).add_constant_properties({"c_prop1": "fire_nation"})
    graph.node(4).add_constant_properties({"c_prop1": "fire_nation"})

    graph.edge(1, 2).add_constant_properties({"c_prop1": "water_tribe"})
    graph.edge(2, 3).add_constant_properties({"c_prop1": "water_tribe"})

    return graph

def test_property_filter_nodes():
    graph = build_graph()

    assert graph.filter_nodes(filter.Property("node_str") == "first").nodes.id == [1]
    assert graph.filter_nodes(filter.Property("node_str") == "first").edges.id == []
    assert graph.filter_nodes(filter.Property("node_str") != "first").nodes.id == [2, 3, 4]
    assert graph.filter_nodes(filter.Property("node_str") != "first").edges.id == [(2, 3), (3, 4)]
    assert graph.filter_nodes(filter.Property("node_bool").is_some()).nodes.id == [4]
    assert graph.filter_nodes(filter.Property("node_bool").is_none()).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_nodes(filter.Property("node_int") == 2).nodes.id == [
        2
    ]  # only looks at the latest value
    assert graph.filter_nodes(filter.Property("node_int") != 1).edges.id == [(2, 3), (3, 4)]
    assert graph.filter_nodes(filter.Property("node_int") > 2).edges.id == [(3, 4)]
    assert graph.filter_nodes(filter.Property("node_int") >= 1).edges.id == [
        (1, 2),
        (2, 3),
        (3, 4),
    ]
    assert graph.filter_nodes(filter.Property("node_int") < 3).edges.id == [(1, 2)]
    assert graph.filter_nodes(filter.Property("node_int") <= 2).edges.id == [(1, 2)]

    assert graph.filter_nodes(filter.Property("node_bool") == True).nodes.id == [4]


def test_property_filter_edges():
    graph = build_graph()

    assert graph.filter_edges(filter.Property("test_str") == "first").edges.id == [(1, 2)]
    # is this the answer we want?, currently excludes edges that don't have the property
    assert graph.filter_edges(filter.Property("test_str") != "first").edges.id == [(2, 3)]
    assert graph.filter_edges(filter.Property("test_str").is_some()).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_edges(filter.Property("test_str").is_none()).edges.id == [(3, 4)]
    assert graph.filter_edges(filter.Property("test_str") == "second").edges.id == []
    assert graph.before(5).filter_edges(filter.Property("test_str") == "second").edges.id == [
        (2, 3)
    ]
    assert graph.filter_edges(filter.Property("test_str").includes(["first", "fourth"])).edges.id == [
        (1, 2)
    ]
    assert graph.filter_edges(filter.Property("test_str").excludes(["first"])).edges.id == [
        (2, 3),
    ]
    assert (
        graph.filter_edges(filter.Property("test_int") == 2).edges.id == []
    )  # only looks at the latest value
    assert graph.filter_edges(filter.Property("test_int") != 1).edges.id == [(1, 2), (3, 4)]
    assert graph.filter_edges(filter.Property("test_int") > 2).edges.id == [(3, 4)]
    assert graph.filter_edges(filter.Property("test_int") >= 1).edges.id == [(2, 3), (3, 4)]
    assert graph.filter_edges(filter.Property("test_int") < 3).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_edges(filter.Property("test_int") <= 1).edges.id == [(1, 2), (2, 3)]

    assert graph.filter_edges(filter.Property("test_bool") == True).edges.id == [
        (2, 3)
    ]  # worth adding special support for this?


def test_filter_exploded_edges():
    graph = build_graph()

    assert graph.filter_exploded_edges(Prop("test_str") == "first").edges.id == [(1, 2)]
    # is this the answer we want?, currently excludes edges that don't have the property
    assert graph.filter_exploded_edges(Prop("test_str") != "first").edges.id == [(2, 3)]
    assert graph.filter_exploded_edges(Prop("test_str").is_some()).edges.id == [
        (1, 2),
        (2, 3),
    ]
    assert graph.filter_exploded_edges(Prop("test_str").is_none()).edges.id == [
        (2, 3),
        (3, 4),
    ]
    assert graph.filter_exploded_edges(Prop("test_str") == "second").edges.id == [
        (2, 3)
    ]
    assert graph.filter_exploded_edges(
        Prop("test_str").any({"first", "fourth"})
    ).edges.id == [(1, 2)]
    assert graph.filter_exploded_edges(
        Prop("test_str").not_any({"first"})
    ).edges.id == [(2, 3)]

    assert graph.filter_exploded_edges(Prop("test_int") == 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") != 2).edges.id == [
        (1, 2),
        (2, 3),
        (3, 4),
    ]
    assert graph.filter_exploded_edges(Prop("test_int") > 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") >= 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") < 3).edges.id == [
        (1, 2),
        (2, 3),
        (3, 4),
    ]
    assert graph.filter_exploded_edges(Prop("test_int") <= 1).edges.id == [
        (1, 2),
        (2, 3),
    ]

    assert graph.filter_exploded_edges(Prop("test_bool") == True).edges.id == [
        (2, 3)
    ]  # worth adding special support for this?
