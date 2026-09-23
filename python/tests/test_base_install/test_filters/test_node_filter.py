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
        filter.Node.degree() < value,
        "both",
        lambda d: d < value,
        f"BOTH < {value}",
    )
    assert_filter(
        graph,
        filter.Node.degree() <= value,
        "both",
        lambda d: d <= value,
        f"BOTH <= {value}",
    )
    assert_filter(
        graph,
        filter.Node.degree() == value,
        "both",
        lambda d: d == value,
        f"BOTH == {value}",
    )
    assert_filter(
        graph,
        filter.Node.degree() != value,
        "both",
        lambda d: d != value,
        f"BOTH != {value}",
    )
    assert_filter(
        graph,
        filter.Node.degree() >= value,
        "both",
        lambda d: d >= value,
        f"BOTH >= {value}",
    )
    assert_filter(
        graph,
        filter.Node.degree() > value,
        "both",
        lambda d: d > value,
        f"BOTH > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_with_float_constants(value):
    # A float constant is compared as written: a whole float names that number,
    # a fraction is never rounded to the degree's type.
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    whole = float(value)
    half = value + 0.5
    for expr, expected, context in [
        (filter.Node.degree() == whole, lambda d: d == value, "== whole"),
        (filter.Node.degree() >= whole, lambda d: d >= value, ">= whole"),
        (filter.Node.degree() < half, lambda d: d <= value, "< half"),
        (filter.Node.degree() <= half, lambda d: d <= value, "<= half"),
        (filter.Node.degree() == half, lambda d: False, "== half"),
        (filter.Node.degree() != half, lambda d: True, "!= half"),
        (filter.Node.degree() >= half, lambda d: d > value, ">= half"),
        (filter.Node.degree() > half, lambda d: d > value, "> half"),
        (filter.Node.degree().is_in([half, whole]), lambda d: d == value, "is_in"),
        (
            filter.Node.degree().is_not_in([half, whole]),
            lambda d: d != value,
            "is_not_in",
        ),
    ]:
        assert_filter(graph, expr, "both", expected, context)


def test_degree_filter_refuses_string_constants():
    # A string never compares with a degree, even one that spells a number; in
    # a set it is simply not a member.
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    for op in ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"):
        with pytest.raises(TypeError, match="of type Str cannot be compared with U64"):
            getattr(filter.Node.degree(), op)("3")
    assert_filter(
        graph,
        filter.Node.degree().is_in(["3", 4]),
        "both",
        lambda d: d == 4,
        "is_in(string, number)",
    )
    assert_filter(
        graph,
        filter.Node.degree().is_in(["3", "4"]),
        "both",
        lambda d: False,
        "is_in(strings)",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_in_direction_comparison(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        filter.Node.in_degree() < value,
        "in",
        lambda d: d < value,
        f"IN < {value}",
    )
    assert_filter(
        graph,
        filter.Node.in_degree() <= value,
        "in",
        lambda d: d <= value,
        f"IN <= {value}",
    )
    assert_filter(
        graph,
        filter.Node.in_degree() == value,
        "in",
        lambda d: d == value,
        f"IN == {value}",
    )
    assert_filter(
        graph,
        filter.Node.in_degree() != value,
        "in",
        lambda d: d != value,
        f"IN != {value}",
    )
    assert_filter(
        graph,
        filter.Node.in_degree() >= value,
        "in",
        lambda d: d >= value,
        f"IN >= {value}",
    )
    assert_filter(
        graph,
        filter.Node.in_degree() > value,
        "in",
        lambda d: d > value,
        f"IN > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_out_direction_comparison(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        filter.Node.out_degree() < value,
        "out",
        lambda d: d < value,
        f"OUT < {value}",
    )
    assert_filter(
        graph,
        filter.Node.out_degree() <= value,
        "out",
        lambda d: d <= value,
        f"OUT <= {value}",
    )
    assert_filter(
        graph,
        filter.Node.out_degree() == value,
        "out",
        lambda d: d == value,
        f"OUT == {value}",
    )
    assert_filter(
        graph,
        filter.Node.out_degree() != value,
        "out",
        lambda d: d != value,
        f"OUT != {value}",
    )
    assert_filter(
        graph,
        filter.Node.out_degree() >= value,
        "out",
        lambda d: d >= value,
        f"OUT >= {value}",
    )
    assert_filter(
        graph,
        filter.Node.out_degree() > value,
        "out",
        lambda d: d > value,
        f"OUT > {value}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_and(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node.degree() > value) & (filter.Node.degree() < value + 5),
        "both",
        lambda d: d > value and d < (value + 5),
        f"BOTH > {value} AND BOTH < {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.in_degree() > value) & (filter.Node.in_degree() < value + 5),
        "in",
        lambda d: d > value and d < (value + 5),
        f"IN > {value} AND IN < {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.out_degree() > value) & (filter.Node.out_degree() < value + 5),
        "out",
        lambda d: d > value and d < (value + 5),
        f"OUT > {value} AND OUT < {value + 5}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_or(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node.degree() < value) | (filter.Node.degree() > value + 5),
        "both",
        lambda d: d < value or d > (value + 5),
        f"BOTH < {value} OR BOTH > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.in_degree() < value) | (filter.Node.in_degree() > value + 5),
        "in",
        lambda d: d < value or d > (value + 5),
        f"IN < {value} OR IN > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.out_degree() < value) | (filter.Node.out_degree() > value + 5),
        "out",
        lambda d: d < value or d > (value + 5),
        f"OUT < {value} OR OUT > {value + 5}",
    )


@pytest.mark.parametrize("value", range(0, 15))
def test_degree_filter_not(value):
    graph = degree_graph_with_add_node_and_add_edge(Graph())

    assert_filter(
        graph,
        (filter.Node.degree() < value) | (~(filter.Node.degree() > value + 5)),
        "both",
        lambda d: d < value or d <= (value + 5),
        f"BOTH < {value} OR BOTH > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.in_degree() < value) | (~(filter.Node.in_degree() > value + 5)),
        "in",
        lambda d: d < value or d <= (value + 5),
        f"IN < {value} OR IN > {value + 5}",
    )
    assert_filter(
        graph,
        (filter.Node.out_degree() < value) | (~(filter.Node.out_degree() > value + 5)),
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
        filter.Node.degree().is_in(set_values),
        "both",
        lambda d: d in set_values,
        f"BOTH is_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node.in_degree().is_in(set_values),
        "in",
        lambda d: d in set_values,
        f"IN is_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node.out_degree().is_in(set_values),
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
        filter.Node.degree().is_not_in(set_values),
        "both",
        lambda d: d not in set_values,
        f"BOTH is_not_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node.in_degree().is_not_in(set_values),
        "in",
        lambda d: d not in set_values,
        f"IN is_not_in({value}, {value + 1})",
    )
    assert_filter(
        graph,
        filter.Node.out_degree().is_not_in(set_values),
        "out",
        lambda d: d not in set_values,
        f"OUT is_not_in({value}, {value + 1})",
    )


def test_degree_filter_with_invalid_expressions():
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    invalid_filters = [
        lambda: filter.Node.degree().is_none(),
        lambda: filter.Node.degree().is_some(),
        lambda: filter.Node.degree().starts_with("1"),
        lambda: filter.Node.degree().ends_with("1"),
        lambda: filter.Node.degree().contains("1"),
        lambda: filter.Node.degree().not_contains("1"),
        lambda: filter.Node.degree().fuzzy_search("1", 1, False),
        lambda: filter.Node.in_degree().is_none(),
        lambda: filter.Node.in_degree().is_some(),
        lambda: filter.Node.in_degree().starts_with("1"),
        lambda: filter.Node.in_degree().ends_with("1"),
        lambda: filter.Node.in_degree().contains("1"),
        lambda: filter.Node.in_degree().not_contains("1"),
        lambda: filter.Node.in_degree().fuzzy_search("1", 1, False),
        lambda: filter.Node.out_degree().is_none(),
        lambda: filter.Node.out_degree().is_some(),
        lambda: filter.Node.out_degree().starts_with("1"),
        lambda: filter.Node.out_degree().ends_with("1"),
        lambda: filter.Node.out_degree().contains("1"),
        lambda: filter.Node.out_degree().not_contains("1"),
        lambda: filter.Node.out_degree().fuzzy_search("1", 1, False),
        lambda: (filter.Node.degree() == 1).any(),
        lambda: (filter.Node.degree() == 1).all(),
        lambda: filter.Node.degree().len() > 0,
        lambda: filter.Node.degree().sum() == 1,
        lambda: filter.Node.degree().avg() == 1,
        lambda: filter.Node.degree().min() == 1,
        lambda: filter.Node.degree().max() == 1,
        lambda: filter.Node.degree().first() == 1,
        lambda: filter.Node.degree().last() == 1,
        lambda: (filter.Node.in_degree() == 1).any(),
        lambda: (filter.Node.in_degree() == 1).all(),
        lambda: filter.Node.in_degree().len() > 0,
        lambda: filter.Node.in_degree().sum() == 1,
        lambda: filter.Node.in_degree().avg() == 1,
        lambda: filter.Node.in_degree().min() == 1,
        lambda: filter.Node.in_degree().max() == 1,
        lambda: filter.Node.in_degree().first() == 1,
        lambda: filter.Node.in_degree().last() == 1,
        lambda: (filter.Node.out_degree() == 1).any(),
        lambda: (filter.Node.out_degree() == 1).all(),
        lambda: filter.Node.out_degree().len() > 0,
        lambda: filter.Node.out_degree().sum() == 1,
        lambda: filter.Node.out_degree().avg() == 1,
        lambda: filter.Node.out_degree().min() == 1,
        lambda: filter.Node.out_degree().max() == 1,
        lambda: filter.Node.out_degree().first() == 1,
        lambda: filter.Node.out_degree().last() == 1,
    ]

    for make_filter in invalid_filters:
        with pytest.raises(
            Exception, match=r"Invalid filter|not comparable|always has a value"
        ):
            graph.filter(make_filter()).nodes.id


@pytest.mark.parametrize("value_a", ["a", "foo"])
def test_degree_filter_with_invalid_string_values(value_a):
    graph = degree_graph_with_add_node_and_add_edge(Graph())
    invalid_filters = [
        lambda: filter.Node.degree() < value_a,
        lambda: filter.Node.degree() <= value_a,
        lambda: filter.Node.degree() == value_a,
        lambda: filter.Node.degree() != value_a,
        lambda: filter.Node.degree() >= value_a,
        lambda: filter.Node.degree() > value_a,
        lambda: filter.Node.in_degree() < value_a,
        lambda: filter.Node.in_degree() <= value_a,
        lambda: filter.Node.in_degree() == value_a,
        lambda: filter.Node.in_degree() != value_a,
        lambda: filter.Node.in_degree() >= value_a,
        lambda: filter.Node.in_degree() > value_a,
        lambda: filter.Node.out_degree() < value_a,
        lambda: filter.Node.out_degree() <= value_a,
        lambda: filter.Node.out_degree() == value_a,
        lambda: filter.Node.out_degree() != value_a,
        lambda: filter.Node.out_degree() >= value_a,
        lambda: filter.Node.out_degree() > value_a,
    ]

    for make_filter in invalid_filters:
        # Mistyped constants fail at the comparison when the expression type is
        # statically known, and at filter() otherwise.
        with pytest.raises(Exception, match=r"Invalid filter|not comparable"):
            graph.filter(make_filter()).nodes.id


@with_variants(init_graph)
def test_filter_nodes_for_node_name_eq():
    def check(graph):
        filter_expr = filter.Node.name() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_ne():
    def check(graph):
        filter_expr = filter.Node.name() != "2"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_in():
    def check(graph):
        filter_expr = filter.Node.name().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.name().is_in(["2", "3"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_not_in():
    def check(graph):
        filter_expr = filter.Node.name().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_eq():
    def check(graph):
        filter_expr = filter.Node.node_type() == "fire_nation"
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
    assert isinstance(filter.Node.node_type() == "person", filter.Expr)


@with_variants(init_graph)
def test_filter_nodes_for_node_type_ne():
    def check(graph):
        filter_expr = filter.Node.node_type() != "fire_nation"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_in():
    def check(graph):
        filter_expr = filter.Node.node_type().is_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().is_in(["fire_nation", "air_nomads"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_not_in():
    def check(graph):
        filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_starts_with():
    def check(graph):
        filter_expr = filter.Node.node_type().starts_with("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().starts_with("Liar")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_ends_with():
    def check(graph):
        filter_expr = filter.Node.node_type().ends_with("tion")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().ends_with("station")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_contains():
    def check(graph):
        filter_expr = filter.Node.node_type().contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_type_not_contains():
    def check(graph):
        filter_expr = filter.Node.node_type().not_contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_fuzzy_search():
    def check(graph):
        filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, True)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().fuzzy_search("air_noma", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_for_not_node_type():
    def check(graph):
        filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node.id() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node.id() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [3]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node.id() != "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node.id() != 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1, 2, 4]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node.id().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node.id().is_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node.id().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node.id().is_not_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [2, 3, 4]
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_with_str_ids_error():
    def check(graph):
        filter_expr = filter.Node.id() == 3
        with pytest.raises(
            Exception,
            match=r"Invalid filter: value I64\(3\) of type I64 cannot be compared with Str",
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_variants(init_graph2)
def test_filter_nodes_with_num_ids_error():
    def check(graph):
        filter_expr = filter.Node.id() == "3"
        with pytest.raises(
            Exception,
            match=r'value Str\(ArcStr\("3"\)\) of type Str cannot be compared with U64',
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_variants(init_graph)
def test_filter_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node.is_active()
        result_ids = sorted(graph.window(1, 4).filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node.is_active()
        result_ids = sorted(graph.window(1, 4).nodes[filter_expr].id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_windowed_is_active():
    def check(graph):
        filter_expr = filter.Node.window(1, 2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2"])
        assert result_ids == expected_ids

    return check


@with_variants(create_test_graph)
def test_filter_nodes_windowed_is_active_not():
    def check(graph):
        filter_expr = filter.Node.window(1, 2).is_active()
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = sorted([])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.latest().is_active()
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph, variants=["graph"])
def test_filter_nodes_snapshot_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(
            ["1", "2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        )
        assert result_ids == expected_ids

    return check


@with_variants(init_graph, variants=["persistent_graph"])
def test_filter_nodes_snapshot_latest_is_active_persistent():
    def check(graph):
        filter_expr = filter.Node.snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_filter_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node.at(2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3"])
        assert result_ids == expected_ids

    return check


@with_variants(init_graph)
def test_select_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node.at(2).is_active()
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

    filter_expr = filter.Node.by_state_column(actual, "bool_col")
    result_ids = sorted(graph.filter(filter_expr).nodes.id)
    expected_ids = sorted(i for i, v in expected.items() if v["bool_col"])
    assert result_ids == expected_ids

    result_ids = sorted(graph.nodes[filter_expr].id)
    assert result_ids == expected_ids


@with_variants(init_graph)
def test_filter_nodes_for_node_name_all_is_invalid():
    def check(graph):
        # The expression builds (the python surface is one Expr type); applying
        # it rejects the qualifier on a scalar field.
        with pytest.raises(Exception, match=r"cannot be compared with Str"):
            filter_expr = (filter.Node.name() == True).all()
            graph.filter(filter_expr).nodes.id

    return check


@with_variants(init_graph)
def test_filter_nodes_for_node_name_len_is_invalid():
    def check(graph):
        filter_expr = filter.Node.name().len() == 1
        with pytest.raises(
            Exception,
            match=r"len\(\) is not valid on a scalar expression of type Str",
        ):
            graph.filter(filter_expr).nodes.id

    return check
