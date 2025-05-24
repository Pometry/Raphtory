from raphtory import Graph, PersistentGraph, Prop
from raphtory import filter
import pytest


def build_graph():
    graph = Graph()

    graph.add_node(0, 1, {"node_str": "first", "node_int": 1})
    graph.add_node(1, 1, {"node_str": "second", "node_int": 2})
    graph.add_node(1, 2, {"node_str": "second", "node_int": 2})
    graph.add_node(2, 3, {"node_str": "third", "node_int": 3})
    graph.add_node(3, 4, {"node_str": "fourth", "node_int": 4, "node_bool": True})

    graph.node(1).add_constant_properties({"c_prop1": "fire_nation"})
    graph.node(2).add_constant_properties({"c_prop1": "water_tribe"})
    graph.node(3).add_constant_properties({"c_prop1": "fire_nation"})
    graph.node(4).add_constant_properties({"c_prop1": "fire_nation"})

    graph.add_edge(0, 1, 2, {"test_str": "first", "test_int": 0})
    graph.add_edge(1, 2, 3, {"test_str": "second", "test_int": 1})
    graph.add_edge(2, 3, 4, {"test_int": 2})
    graph.add_edge(3, 3, 4, {"test_int": 3})
    graph.add_edge(4, 2, 3, {"test_bool": True})
    graph.add_edge(5, 2, 3, {"test_str": "third"})

    graph.edge(1, 2).add_constant_properties({"c_prop1": "water_tribe"})
    graph.edge(2, 3).add_constant_properties({"c_prop1": "water_tribe"})

    return graph


def test_property_filter_nodes():
    graph = build_graph()

    test_node_cases = [
        (filter.Property("node_str") == "first", []),
        (filter.Property("node_str") != "first", [1, 2, 3, 4]),
        (filter.Property("node_bool").is_some(), [4]),
        (filter.Property("node_bool").is_none(), [1, 2, 3]),
        (filter.Property("node_int") == 2, [1, 2]),
        (filter.Property("node_bool") == True, [4]),
    ]

    for filter_expr, expected_ids in test_node_cases:
        assert sorted(graph.filter_nodes(filter_expr).nodes.id) == sorted(expected_ids)

    test_edge_cases = [
        (filter.Property("node_str") == "first", []),
        (filter.Property("node_str") != "first", [(1, 2), (2, 3), (3, 4)]),
        (filter.Property("node_bool").is_none(), [(1, 2), (2, 3)]),
        (filter.Property("node_int") != 1, [(1, 2), (2, 3), (3, 4)]),
        (filter.Property("node_int") > 2, [(3, 4)]),
        (filter.Property("node_int") >= 1, [(1, 2), (2, 3), (3, 4)]),
        (filter.Property("node_int") < 3, [(1, 2)]),
        (filter.Property("node_int") <= 2, [(1, 2)]),
    ]

    for filter_expr, expected_ids in test_edge_cases:
        assert sorted(graph.filter_nodes(filter_expr).edges.id) == sorted(expected_ids)


def test_property_filter_edges():
    graph = build_graph()

    test_cases = [
        (filter.Property("test_str") == "first", [(1, 2)]),
        (
            filter.Property("test_str") != "first",
            [(2, 3)],
        ),  # currently excludes edges without the property
        (filter.Property("test_str").is_some(), [(1, 2), (2, 3)]),
        (filter.Property("test_str").is_none(), [(3, 4)]),
        (filter.Property("test_str") == "second", []),
        (filter.Property("test_str").is_in(["first", "fourth"]), [(1, 2)]),
        (filter.Property("test_str").is_not_in(["first"]), [(2, 3)]),
        (filter.Property("test_int") == 2, []),
        (filter.Property("test_int") != 1, [(1, 2), (3, 4)]),
        (filter.Property("test_int") > 2, [(3, 4)]),
        (filter.Property("test_int") >= 1, [(2, 3), (3, 4)]),
        (filter.Property("test_int") < 3, [(1, 2), (2, 3)]),
        (filter.Property("test_int") <= 1, [(1, 2), (2, 3)]),
        (filter.Property("test_bool") == True, [(2, 3)]),
    ]

    for filter_expr, expected_ids in test_cases:
        assert sorted(graph.filter_edges(filter_expr).edges.id) == sorted(expected_ids)

    # edge case: temporal filtering before time 5
    filter_expr = filter.Property("test_str") == "second"
    expected_ids = [(2, 3)]
    assert sorted(graph.before(5).filter_edges(filter_expr).edges.id) == sorted(
        expected_ids
    )


@pytest.mark.skip(reason="Ignoring this test temporarily")
def test_filter_exploded_edges():
    graph = build_graph()

    test_cases = [
        (Prop("test_str") == "first", [(1, 2)]),
        (
            Prop("test_str") != "first",
            [(2, 3)],
        ),  # currently excludes edges without the property
        (Prop("test_str").is_some(), [(1, 2), (2, 3)]),
        (Prop("test_str").is_none(), [(2, 3), (3, 4)]),
        (Prop("test_str") == "second", [(2, 3)]),
        (Prop("test_str").is_in({"first", "fourth"}), [(1, 2)]),
        (Prop("test_str").is_not_in({"first"}), [(2, 3)]),
        (Prop("test_int") == 2, [(3, 4)]),
        (Prop("test_int") != 2, [(1, 2), (2, 3), (3, 4)]),
        (Prop("test_int") > 2, [(3, 4)]),
        (Prop("test_int") >= 2, [(3, 4)]),
        (Prop("test_int") < 3, [(1, 2), (2, 3), (3, 4)]),
        (Prop("test_int") <= 1, [(1, 2), (2, 3)]),
        (Prop("test_bool") == True, [(2, 3)]),  # worth adding special support for this?
    ]

    for filter_expr, expected_ids in test_cases:
        assert sorted(graph.filter_exploded_edges(filter_expr).edges.id) == sorted(
            expected_ids
        )
