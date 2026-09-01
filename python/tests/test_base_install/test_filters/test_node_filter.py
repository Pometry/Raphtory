from raphtory import Graph, filter
from filters_setup import (
    create_test_graph,
    degree_graph_with_add_node_and_add_edge,
    init_graph,
    init_graph2,
)
from utils import with_variants
import pytest


def sort_vids(vids):
    return sorted(list(vids))


def candidates_with_history_after_filtering(graph, candidate_nodes):
    subgraph = graph.subgraph(candidate_nodes)
    return sort_vids([n.id for n in subgraph.nodes if len(n.history.collect()) > 0])


def assert_filter(graph, filter_expr, metric, manual_expr, context):
    def metric_value(node):
        if metric == "both":
            return node.degree()
        if metric == "in":
            return node.in_degree()
        if metric == "out":
            return node.out_degree()
        raise ValueError(f"Unknown metric '{metric}' in {context}")

    expected_select_nodes = [n.id for n in graph.nodes if manual_expr(metric_value(n))]
    expected_select_nodes = sort_vids(expected_select_nodes)

    expected_filter_nodes = candidates_with_history_after_filtering(
        graph, expected_select_nodes
    )

    filtered_event_nodes = sort_vids(graph.filter(filter_expr).nodes.id)
    assert (
        filtered_event_nodes == expected_filter_nodes
    ), f"{context} failed for event graph"

    selected_event_nodes = sort_vids(graph.nodes[filter_expr].id)
    assert (
        selected_event_nodes == expected_select_nodes
    ), f"{context} failed for event graph select"

    persistent_graph = graph.persistent_graph()

    filtered_persistent_nodes = sort_vids(persistent_graph.filter(filter_expr).nodes.id)
    assert (
        filtered_persistent_nodes == expected_filter_nodes
    ), f"{context} failed for persistent graph"

    selected_persistent_nodes = sort_vids(persistent_graph.nodes[filter_expr].id)
    assert (
        selected_persistent_nodes == expected_select_nodes
    ), f"{context} failed for persistent graph select"


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_both_direction_comparison(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        filter.Node().degree() < value,
        "both",
        lambda d: d < value,
        f"BOTH < {value}",
    )
    assert_filter(
        graph,
        filter.Node().degree() <= value,
        "both",
        lambda d: d <= value,
        f"BOTH <= {value}",
    )
    assert_filter(
        graph,
        filter.Node().degree() == value,
        "both",
        lambda d: d == value,
        f"BOTH == {value}",
    )
    assert_filter(
        graph,
        filter.Node().degree() != value,
        "both",
        lambda d: d != value,
        f"BOTH != {value}",
    )
    assert_filter(
        graph,
        filter.Node().degree() >= value,
        "both",
        lambda d: d >= value,
        f"BOTH >= {value}",
    )
    assert_filter(
        graph,
        filter.Node().degree() > value,
        "both",
        lambda d: d > value,
        f"BOTH > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_in_direction_comparison(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        filter.Node().in_degree() < value,
        "in",
        lambda d: d < value,
        f"IN < {value}",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() <= value,
        "in",
        lambda d: d <= value,
        f"IN <= {value}",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() == value,
        "in",
        lambda d: d == value,
        f"IN == {value}",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() != value,
        "in",
        lambda d: d != value,
        f"IN != {value}",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() >= value,
        "in",
        lambda d: d >= value,
        f"IN >= {value}",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() > value,
        "in",
        lambda d: d > value,
        f"IN > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_out_direction_comparison(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        filter.Node().out_degree() < value,
        "out",
        lambda d: d < value,
        f"OUT < {value}",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() <= value,
        "out",
        lambda d: d <= value,
        f"OUT <= {value}",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() == value,
        "out",
        lambda d: d == value,
        f"OUT == {value}",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() != value,
        "out",
        lambda d: d != value,
        f"OUT != {value}",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() >= value,
        "out",
        lambda d: d >= value,
        f"OUT >= {value}",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() > value,
        "out",
        lambda d: d > value,
        f"OUT > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_and(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node().degree() > value) & (filter.Node().degree() < value + 5),
        "both",
        lambda d: d > value and d < (value + 5),
        f"BOTH > {value} AND BOTH < {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().in_degree() > value) & (filter.Node().in_degree() < value + 5),
        "in",
        lambda d: d > value and d < (value + 5),
        f"IN > {value} AND IN < {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().out_degree() > value) & (filter.Node().out_degree() < value + 5),
        "out",
        lambda d: d > value and d < (value + 5),
        f"OUT > {value} AND OUT < {value + 5}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_or(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node().degree() < value) | (filter.Node().degree() > value + 5),
        "both",
        lambda d: d < value or d > (value + 5),
        f"BOTH < {value} OR BOTH > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().in_degree() < value) | (filter.Node().in_degree() > value + 5),
        "in",
        lambda d: d < value or d > (value + 5),
        f"IN < {value} OR IN > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().out_degree() < value) | (filter.Node().out_degree() > value + 5),
        "out",
        lambda d: d < value or d > (value + 5),
        f"OUT < {value} OR OUT > {value + 5}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_not(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node().degree() < value) | (~(filter.Node().degree() > value + 5)),
        "both",
        lambda d: d < value or d <= (value + 5),
        f"BOTH < {value} OR BOTH > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().in_degree() < value) | (~(filter.Node().in_degree() > value + 5)),
        "in",
        lambda d: d < value or d <= (value + 5),
        f"IN < {value} OR IN > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node().out_degree() < value) | (~(filter.Node().out_degree() > value + 5)),
        "out",
        lambda d: d < value or d <= (value + 5),
        f"OUT < {value} OR OUT > {value + 5}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_is_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    set_values = [value, value + 1]

    assert_filter(
        graph,
        filter.Node().degree().is_in(set_values),
        "both",
        lambda d: d in set_values,
        f"BOTH is_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_in(set_values),
        "in",
        lambda d: d in set_values,
        f"IN is_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_in(set_values),
        "out",
        lambda d: d in set_values,
        f"OUT is_in({value}, {value + 1})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_is_not_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    set_values = [value, value + 1]

    assert_filter(
        graph,
        filter.Node().degree().is_not_in(set_values),
        "both",
        lambda d: d not in set_values,
        f"BOTH is_not_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_not_in(set_values),
        "in",
        lambda d: d not in set_values,
        f"IN is_not_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_not_in(set_values),
        "out",
        lambda d: d not in set_values,
        f"OUT is_not_in({value}, {value + 1})",
    )


def test_degree_filter_with_invalid_expressions():
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    invalid_filters = [
        filter.Node().degree().is_none(),
        filter.Node().degree().is_some(),
        filter.Node().degree().starts_with("1"),
        filter.Node().degree().ends_with("1"),
        filter.Node().degree().contains("1"),
        filter.Node().degree().not_contains("1"),
        filter.Node().degree().fuzzy_search("1", 1, False),
        filter.Node().in_degree().is_none(),
        filter.Node().in_degree().is_some(),
        filter.Node().in_degree().starts_with("1"),
        filter.Node().in_degree().ends_with("1"),
        filter.Node().in_degree().contains("1"),
        filter.Node().in_degree().not_contains("1"),
        filter.Node().in_degree().fuzzy_search("1", 1, False),
        filter.Node().out_degree().is_none(),
        filter.Node().out_degree().is_some(),
        filter.Node().out_degree().starts_with("1"),
        filter.Node().out_degree().ends_with("1"),
        filter.Node().out_degree().contains("1"),
        filter.Node().out_degree().not_contains("1"),
        filter.Node().out_degree().fuzzy_search("1", 1, False),
        filter.Node().degree().any() == 1,
        filter.Node().degree().all() == 1,
        filter.Node().degree().len() > 0,
        filter.Node().degree().sum() == 1,
        filter.Node().degree().avg() == 1,
        filter.Node().degree().min() == 1,
        filter.Node().degree().max() == 1,
        filter.Node().degree().first() == 1,
        filter.Node().degree().last() == 1,
        filter.Node().in_degree().any() == 1,
        filter.Node().in_degree().all() == 1,
        filter.Node().in_degree().len() > 0,
        filter.Node().in_degree().sum() == 1,
        filter.Node().in_degree().avg() == 1,
        filter.Node().in_degree().min() == 1,
        filter.Node().in_degree().max() == 1,
        filter.Node().in_degree().first() == 1,
        filter.Node().in_degree().last() == 1,
        filter.Node().out_degree().any() == 1,
        filter.Node().out_degree().all() == 1,
        filter.Node().out_degree().len() > 0,
        filter.Node().out_degree().sum() == 1,
        filter.Node().out_degree().avg() == 1,
        filter.Node().out_degree().min() == 1,
        filter.Node().out_degree().max() == 1,
        filter.Node().out_degree().first() == 1,
        filter.Node().out_degree().last() == 1,
    ]

    for filter_expr in invalid_filters:
        with pytest.raises(Exception, match=r"Invalid filter"):
            graph.filter(filter_expr).nodes.id


@pytest.mark.parametrize("value_a, value_b", [("a", "b"), ("foo", "bar")])
def test_degree_filter_with_invalid_string_values(value_a, value_b):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    invalid_filters = [
        filter.Node().degree() < value_a,
        filter.Node().degree() <= value_a,
        filter.Node().degree() == value_a,
        filter.Node().degree() != value_a,
        filter.Node().degree() >= value_a,
        filter.Node().degree() > value_a,
        filter.Node().in_degree() < value_a,
        filter.Node().in_degree() <= value_a,
        filter.Node().in_degree() == value_a,
        filter.Node().in_degree() != value_a,
        filter.Node().in_degree() >= value_a,
        filter.Node().in_degree() > value_a,
        filter.Node().out_degree() < value_a,
        filter.Node().out_degree() <= value_a,
        filter.Node().out_degree() == value_a,
        filter.Node().out_degree() != value_a,
        filter.Node().out_degree() >= value_a,
        filter.Node().out_degree() > value_a,
        filter.Node().degree().is_in([value_a, value_b]),
        filter.Node().degree().is_not_in([value_a, value_b]),
        filter.Node().in_degree().is_in([value_a, value_b]),
        filter.Node().in_degree().is_not_in([value_a, value_b]),
        filter.Node().out_degree().is_in([value_a, value_b]),
        filter.Node().out_degree().is_not_in([value_a, value_b]),
    ]

    for filter_expr in invalid_filters:
        with pytest.raises(Exception, match=r"Invalid filter"):
            graph.filter(filter_expr).nodes.id


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_string_threshold(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_str = str(value)
    parsed_str = int(threshold_str)

    assert_filter(
        graph,
        filter.Node().degree() < threshold_str,
        "both",
        lambda d: d < parsed_str,
        f"BOTH < string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().degree() <= threshold_str,
        "both",
        lambda d: d <= parsed_str,
        f"BOTH <= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().degree() == threshold_str,
        "both",
        lambda d: d == parsed_str,
        f"BOTH == string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().degree() != threshold_str,
        "both",
        lambda d: d != parsed_str,
        f"BOTH != string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().degree() >= threshold_str,
        "both",
        lambda d: d >= parsed_str,
        f"BOTH >= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().degree() > threshold_str,
        "both",
        lambda d: d > parsed_str,
        f"BOTH > string threshold parsed to u64 ({threshold_str})",
    )

    assert_filter(
        graph,
        filter.Node().in_degree() < threshold_str,
        "in",
        lambda d: d < parsed_str,
        f"IN < string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() <= threshold_str,
        "in",
        lambda d: d <= parsed_str,
        f"IN <= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() == threshold_str,
        "in",
        lambda d: d == parsed_str,
        f"IN == string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() != threshold_str,
        "in",
        lambda d: d != parsed_str,
        f"IN != string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() >= threshold_str,
        "in",
        lambda d: d >= parsed_str,
        f"IN >= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() > threshold_str,
        "in",
        lambda d: d > parsed_str,
        f"IN > string threshold parsed to u64 ({threshold_str})",
    )

    assert_filter(
        graph,
        filter.Node().out_degree() < threshold_str,
        "out",
        lambda d: d < parsed_str,
        f"OUT < string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() <= threshold_str,
        "out",
        lambda d: d <= parsed_str,
        f"OUT <= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() == threshold_str,
        "out",
        lambda d: d == parsed_str,
        f"OUT == string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() != threshold_str,
        "out",
        lambda d: d != parsed_str,
        f"OUT != string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() >= threshold_str,
        "out",
        lambda d: d >= parsed_str,
        f"OUT >= string threshold parsed to u64 ({threshold_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() > threshold_str,
        "out",
        lambda d: d > parsed_str,
        f"OUT > string threshold parsed to u64 ({threshold_str})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_string_is_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_a_str = str(value)
    threshold_b_str = str(value + 1)
    set_values = [int(threshold_a_str), int(threshold_b_str)]

    assert_filter(
        graph,
        filter.Node().degree().is_in([threshold_a_str, threshold_b_str]),
        "both",
        lambda d: d in set_values,
        f"BOTH is_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_in([threshold_a_str, threshold_b_str]),
        "in",
        lambda d: d in set_values,
        f"IN is_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_in([threshold_a_str, threshold_b_str]),
        "out",
        lambda d: d in set_values,
        f"OUT is_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_string_is_not_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_a_str = str(value)
    threshold_b_str = str(value + 1)
    set_values = [int(threshold_a_str), int(threshold_b_str)]

    assert_filter(
        graph,
        filter.Node().degree().is_not_in([threshold_a_str, threshold_b_str]),
        "both",
        lambda d: d not in set_values,
        f"BOTH is_not_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_not_in([threshold_a_str, threshold_b_str]),
        "in",
        lambda d: d not in set_values,
        f"IN is_not_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_not_in([threshold_a_str, threshold_b_str]),
        "out",
        lambda d: d not in set_values,
        f"OUT is_not_in(string thresholds parsed to u64) ({threshold_a_str}, {threshold_b_str})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_float_threshold(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_float = value + 0.5
    parsed_float = int(threshold_float)

    assert_filter(
        graph,
        filter.Node().degree() < threshold_float,
        "both",
        lambda d: d < parsed_float,
        f"BOTH < float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().degree() <= threshold_float,
        "both",
        lambda d: d <= parsed_float,
        f"BOTH <= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().degree() == threshold_float,
        "both",
        lambda d: d == parsed_float,
        f"BOTH == float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().degree() != threshold_float,
        "both",
        lambda d: d != parsed_float,
        f"BOTH != float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().degree() >= threshold_float,
        "both",
        lambda d: d >= parsed_float,
        f"BOTH >= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().degree() > threshold_float,
        "both",
        lambda d: d > parsed_float,
        f"BOTH > float threshold cast to u64 ({value})",
    )

    assert_filter(
        graph,
        filter.Node().in_degree() < threshold_float,
        "in",
        lambda d: d < parsed_float,
        f"IN < float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() <= threshold_float,
        "in",
        lambda d: d <= parsed_float,
        f"IN <= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() == threshold_float,
        "in",
        lambda d: d == parsed_float,
        f"IN == float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() != threshold_float,
        "in",
        lambda d: d != parsed_float,
        f"IN != float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() >= threshold_float,
        "in",
        lambda d: d >= parsed_float,
        f"IN >= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree() > threshold_float,
        "in",
        lambda d: d > parsed_float,
        f"IN > float threshold cast to u64 ({value})",
    )

    assert_filter(
        graph,
        filter.Node().out_degree() < threshold_float,
        "out",
        lambda d: d < parsed_float,
        f"OUT < float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() <= threshold_float,
        "out",
        lambda d: d <= parsed_float,
        f"OUT <= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() == threshold_float,
        "out",
        lambda d: d == parsed_float,
        f"OUT == float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() != threshold_float,
        "out",
        lambda d: d != parsed_float,
        f"OUT != float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() >= threshold_float,
        "out",
        lambda d: d >= parsed_float,
        f"OUT >= float threshold cast to u64 ({value})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree() > threshold_float,
        "out",
        lambda d: d > parsed_float,
        f"OUT > float threshold cast to u64 ({value})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_float_is_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_a = value + 0.25
    threshold_b = value + 1.75
    set_values = [int(threshold_a), int(threshold_b)]

    assert_filter(
        graph,
        filter.Node().degree().is_in([threshold_a, threshold_b]),
        "both",
        lambda d: d in set_values,
        f"BOTH is_in(float thresholds cast to u64) ({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_in([threshold_a, threshold_b]),
        "in",
        lambda d: d in set_values,
        f"IN is_in(float thresholds cast to u64) ({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_in([threshold_a, threshold_b]),
        "out",
        lambda d: d in set_values,
        f"OUT is_in(float thresholds cast to u64) ({value}, {value + 1})",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_float_is_not_in(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    threshold_a = value + 0.25
    threshold_b = value + 1.75
    set_values = [int(threshold_a), int(threshold_b)]

    assert_filter(
        graph,
        filter.Node().degree().is_not_in([threshold_a, threshold_b]),
        "both",
        lambda d: d not in set_values,
        f"BOTH is_not_in(float thresholds cast to u64) ({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().in_degree().is_not_in([threshold_a, threshold_b]),
        "in",
        lambda d: d not in set_values,
        f"IN is_not_in(float thresholds cast to u64) ({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node().out_degree().is_not_in([threshold_a, threshold_b]),
        "out",
        lambda d: d not in set_values,
        f"OUT is_not_in(float thresholds cast to u64) ({value}, {value + 1})",
    )


@with_variants(init_graph)
def test_filter_nodes_for_node_name_eq():
    def check(graph):
        filter_expr = filter.Node().name() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_ne():
    def check(graph):
        filter_expr = filter.Node().name() != "2"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_in():
    def check(graph):
        filter_expr = filter.Node().name().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr = filter.Node().name().is_in(["2", "3"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_not_in():
    def check(graph):
        filter_expr = filter.Node().name().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_eq():
    def check(graph):
        filter_expr = filter.Node().node_type() == "fire_nation"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


def test_node_type_comparison_to_a_non_string_type_is_a_python_error():
    """`node_type() == 5` never becomes an expression.

    The comparison itself raises, rather than falling back to Python's default
    `==` and yielding a plain `bool`. That fallback was the dangerous shape: it
    turned a mistyped comparison into a value that is not a filter at all, and
    would silently become a match-everything filter the day a bare `bool` is
    accepted as one. Failing at the comparison also puts the error where the
    mistake is, instead of at some later `filter()` call.
    """
    with pytest.raises(TypeError):
        filter.Node.node_type() == 5
    with pytest.raises(TypeError):
        filter.Node.node_type() != 5
    # A correctly typed comparison still builds an expression.
    assert isinstance(filter.Node.node_type() == "person", filter.FilterExpr)


@with_variants(init_graph)
def test_filter_nodes_for_node_type_ne():
    def check(graph):
        filter_expr = filter.Node().node_type() != "fire_nation"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_in():
    def check(graph):
        filter_expr = filter.Node().node_type().is_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node().node_type().is_in(["fire_nation", "air_nomads"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_not_in():
    def check(graph):
        filter_expr = filter.Node().node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_starts_with():
    def check(graph):
        filter_expr = filter.Node().node_type().starts_with("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node().node_type().starts_with("Liar")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_ends_with():
    def check(graph):
        filter_expr = filter.Node().node_type().ends_with("tion")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node().node_type().ends_with("station")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_contains():
    def check(graph):
        filter_expr = filter.Node().node_type().contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_not_contains():
    def check(graph):
        filter_expr = filter.Node().node_type().not_contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_fuzzy_search():
    def check(graph):
        filter_expr = filter.Node().node_type().fuzzy_search("fire", 2, True)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node().node_type().fuzzy_search("fire", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node().node_type().fuzzy_search("air_noma", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_not_node_type():
    def check(graph):
        filter_expr = filter.Node().node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node().id() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node().id() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [3]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node().id() != "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node().id() != 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1, 2, 4]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node().id().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node().id().is_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node().id().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node().id().is_not_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [2, 3, 4]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_error():
    def check(graph):
        filter_expr = filter.Node().id() == 3
        with pytest.raises(
            Exception,
            match='Invalid filter: Filter value type does not match node ID type. Expected Str but got "U64"',
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_error():
    def check(graph):
        filter_expr = filter.Node().id() == "3"
        with pytest.raises(
            Exception,
            match='Invalid filter: Filter value type does not match node ID type. Expected U64 but got "Str"',
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_variants(init_graph)
def test_filter_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node().is_active()
        result_ids = sorted(graph.window(1, 4).filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node().is_active()
        result_ids = sorted(graph.window(1, 4).nodes[filter_expr].id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_windowed_is_active():
    def check(graph):
        filter_expr = filter.Node().window(1, 2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2"])
        assert result_ids == expected_ids

    return check


@with_variants(create_test_graph)
def test_filter_nodes_windowed_is_active_not():
    def check(graph):
        filter_expr = filter.Node().window(1, 2).is_active()
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = sorted([])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node().latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node().latest().is_active()
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph, variants=["graph"])
def test_filter_nodes_snapshot_latest_is_active():
    def check(graph):
        filter_expr = filter.Node().snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(
            ["1", "2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        )
        assert result_ids == expected_ids

    return check


@with_variants(init_graph, variants=["persistent_graph"])
def test_filter_nodes_snapshot_latest_is_active_persistent():
    def check(graph):
        filter_expr = filter.Node().snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node().at(2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node().at(2).is_active()
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted(["1", "2", "3"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_neighbours():
    def check(graph):
        filter_expr = filter.Graph.window(1, 5)
        result_ids = sorted(graph.node(1).neighbours[filter_expr].id)
        expected_ids = [2, 3]
        assert result_ids == expected_ids

    return check


def test_filter_nodes_by_column():
    from raphtory import Graph
    from raphtory.algorithms import alternating_mask

    graph = Graph()
    graph.add_node(1, 1, {})
    graph.add_node(1, 2, {})
    graph.add_node(1, 3, {})
    graph.add_node(1, 4, {})
    graph.add_node(1, 5, {})

    expected = {i: {"bool_col": v % 2 != 0} for (v, i) in enumerate(graph.nodes.id)}
    actual = alternating_mask(graph)
    assert actual == expected

    filter_expr = filter.Node().by_state_column(actual, "bool_col")
    result_ids = sorted(graph.filter(filter_expr).nodes.id)
    expected_ids = sorted(i for i, v in expected.items() if v["bool_col"])
    assert result_ids == expected_ids

    result_ids = sorted(graph.nodes[filter_expr].id)
    assert result_ids == expected_ids


@with_variants(init_graph)
def test_filter_nodes_for_node_name_all_is_invalid():
    def check(graph):
        with pytest.raises(AttributeError, match=r"has no attribute 'all'"):
            filter.Node().name().all()

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_len_is_invalid():
    def check(graph):
        with pytest.raises(AttributeError, match=r"has no attribute 'len'"):
            filter.Node().name().len()

    return check
