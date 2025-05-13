from raphtory import Prop, filter
import pytest
from test_base_install.test_filters.conftest import generate_graph_variants

graph, persistent_graph, event_disk_graph, persistent_disk_graph = generate_graph_variants()


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_node_composite_filter(graph):
    filter_expr1 = filter.Property("p2") == 2
    filter_expr2 = filter.Property("p1") == "kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") > 2
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 | filter_expr2).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p9") < 9
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = ["1"]
    assert result_ids == expected_ids

    filter_expr1 = filter.Node.node_type() == "fire_nation"
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = ["1"]
    assert result_ids == expected_ids

    filter_expr1 = filter.Node.name() == "2"
    filter_expr2 = filter.Property("p2") >= 2
    result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
    expected_ids = ["2"]
    assert result_ids == expected_ids

    filter_expr1 = filter.Node.name() == "2"
    filter_expr2 = filter.Property("p2") == 2
    filter_expr3 = filter.Property("p9") <= 5
    result_ids = sorted(graph.filter_nodes((filter_expr1 & filter_expr2) | filter_expr3).nodes.id)
    expected_ids = ["1", "2"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_not_node_composite_filter(graph):
    filter_expr1 = filter.Node.name() == "2"
    filter_expr2 = filter.Property("p2") >= 2
    result_ids = sorted(graph.filter_nodes(~filter_expr1 & filter_expr2).nodes.id)
    expected_ids = ["3"]
    assert result_ids == expected_ids

    result_ids = sorted(graph.filter_nodes(~(filter_expr1 & filter_expr2)).nodes.id)
    expected_ids = sorted(["1", "3", "4", "David Gilmour",  "Jimmy Page", "John Mayer"])
    assert result_ids == expected_ids
