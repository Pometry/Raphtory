from raphtory import Prop, filter
import pytest
from filters_setup import init_graph
from utils import with_disk_variants


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_eq():
    def check(graph):
        filter_expr = filter.Property("p2") == 2
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_ne():
    def check(graph):
        filter_expr = filter.Property("p2") != 2
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_lt():
    def check(graph):
        filter_expr = filter.Property("p2") < 10
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_le():
    def check(graph):
        filter_expr = filter.Property("p2") <= 6
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_gt():
    def check(graph):
        filter_expr = filter.Property("p2") > 2
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_for_property_ge():
    def check(graph):
        filter_expr = filter.Property("p2") >= 2
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_in():
    def check(graph):
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

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_not_in():
    def check(graph):
        filter_expr = filter.Property("p2").is_not_in([6])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_is_some():
    def check(graph):
        filter_expr = filter.Property("p2").is_some()
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"), ('David Gilmour', 'John Mayer'), ('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_is_none():
    def check(graph):
        filter_expr = filter.Property("p3").is_none()
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1","2"),("2","3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_contains():
    def check(graph):
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

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_not_contains():
    def check(graph):
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

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_not_property():
    def check(graph):
        filter_expr = filter.Property("p3").is_none()
        result_ids = sorted(graph.filter_edges(~filter_expr).edges.id)
        expected_ids = sorted([("2","1"),("3","1"), ("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")])
        assert result_ids == expected_ids

    return check

