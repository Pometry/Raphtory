from raphtory import Prop, filter
import pytest
from test_base_install.test_filters.conftest import generate_graph_variants

graph, persistent_graph, event_disk_graph, persistent_disk_graph = generate_graph_variants()

@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_edge_composite_filter(graph):
    filter_expr1 = filter.Property("p2") == 2
    filter_expr2 = filter.Property("p1") == "kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") > 2
    filter_expr2 = filter.Property("p1") == "shivam_kapoor"
    result_ids = sorted(graph.filter_edges(filter_expr1 | filter_expr2).edges.id)
    expected_ids = sorted([('1', '2'), ('2', '1'), ('3', '1'), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids

# TODO: Enable this test once string property is fixed for disk_storage_graph
#     filter_expr1 = filter.Property("p2") < 9
#     filter_expr2 = filter.Property("p1") == "shivam_kapoor"
#     result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
#     expected_ids = [("1", "2")]
#     assert result_ids == expected_ids

    filter_expr1 = filter.Property("p2") < 9
    filter_expr2 = filter.Property("p3") < 9
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = [("2", "1"), ("3", "1"), ("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")]
    assert result_ids == expected_ids

# TODO: Enable this test once string property is fixed for disk_storage_graph
#     filter_expr1 = filter.Edge.src().name() == "1"
#     filter_expr2 = filter.Property("p1") == "shivam_kapoor"
#     result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
#     expected_ids = [("1", "2")]
#     assert result_ids == expected_ids

    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
    expected_ids = sorted([('2', '1'), ('3', '1')])
    assert result_ids == expected_ids

# TODO: Enable this test once string property is fixed for disk_storage_graph
#     filter_expr1 = filter.Edge.src().name() == "1"
#     filter_expr2 = filter.Property("p1") == "shivam_kapoor"
#     filter_expr3 = filter.Property("p3") == 5
#     result_ids = sorted(graph.filter_edges((filter_expr1 & filter_expr2) | filter_expr3).edges.id)
#     expected_ids = [("1", "2")]
#     assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_not_edge_composite_filter(graph):
    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 2
    result_ids = sorted(graph.filter_edges(~filter_expr1 & filter_expr2).edges.id)
    expected_ids = [('2', '3')]
    assert result_ids == expected_ids

    filter_expr1 = filter.Edge.dst().name() == "1"
    filter_expr2 = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(~(filter_expr1 & filter_expr2)).edges.id)
    expected_ids = sorted([('1', '2'), ('2', '3'), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids
