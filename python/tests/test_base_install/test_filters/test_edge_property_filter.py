from raphtory import Prop, filter
import pytest
from test_base_install.test_filters.conftest import generate_graph_variants

graph, persistent_graph, event_disk_graph, persistent_disk_graph = generate_graph_variants()


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_eq(graph):
    filter_expr = filter.Property("p2") == 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "3")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_ne(graph):
    filter_expr = filter.Property("p2") != 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_lt(graph):
    filter_expr = filter.Property("p2") < 10
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_le(graph):
    filter_expr = filter.Property("p2") <= 6
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_gt(graph):
    filter_expr = filter.Property("p2") > 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_edges_for_property_ge(graph):
    filter_expr = filter.Property("p2") >= 2
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_in(graph):
    filter_expr = filter.Property("p2").is_in([])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr = filter.Property("p2").is_in([0])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr = filter.Property("p2").is_in([6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids

    filter_expr = filter.Property("p2").is_in([2, 6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_not_in(graph):
    filter_expr = filter.Property("p2").is_not_in([6])
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "3")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_is_some(graph):
    filter_expr = filter.Property("p2").is_some()
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_is_none(graph):
    filter_expr = filter.Property("p3").is_none()
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("1","2"),("2","3")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_contains(graph):
    filter_expr = filter.Property("p10").contains("Paper")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1'), ('2','3')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().any().contains("Paper")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1'), ('2','3')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().latest().contains("Paper")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1'), ('2','3')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").constant().contains("Paper")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = []
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_property_not_contains(graph):
    filter_expr = filter.Property("p10").not_contains("ship")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().any().not_contains("ship")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").temporal().latest().not_contains("ship")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = [('1','2'), ('2','1')]
    assert result_ids == expected_ids

    filter_expr = filter.Property("p10").constant().not_contains("ship")
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = []
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_filter_edges_for_not_property(graph):
    filter_expr = filter.Property("p3").is_none()
    result_ids = sorted(graph.filter_edges(~filter_expr).edges.id)
    expected_ids = sorted([("2","1"),("3","1"), ("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")])
    assert result_ids == expected_ids
