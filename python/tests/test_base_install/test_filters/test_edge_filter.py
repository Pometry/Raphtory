from raphtory import Prop, filter
import pytest
from test_base_install.test_filters.conftest import init_graph
from utils import with_disk_variants


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_eq():
    def check(graph):
        filter_expr = filter.Edge.src().name() == "2"
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("2", "1"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_ne():
    def check(graph):
        filter_expr = filter.Edge.src().name() != "1"
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([
            ("2", "1"), ("2", "3"), ("3", "1"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page")
        ])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_in():
    def check(graph):
        filter_expr = filter.Edge.src().name().is_in(["1"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.src().name().is_in(["1", "2"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "1"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_not_in():
    def check(graph):
        filter_expr = filter.Edge.src().name().is_not_in(["1"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([
            ("2", "1"), ("2", "3"), ("3", "1"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page")
        ])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_dst_eq():
    def check(graph):
        filter_expr = filter.Edge.dst().name() == "1"
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("2", "1"), ("3", "1")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_dst_ne():
    def check(graph):
        filter_expr = filter.Edge.dst().name() != "2"
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([
            ("2", "1"), ("2", "3"), ("3", "1"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page")
        ])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_dst_in():
    def check(graph):
        filter_expr = filter.Edge.dst().name().is_in(["2"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.dst().name().is_in(["2", "3"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_dst_not_in():
    def check(graph):
        filter_expr = filter.Edge.dst().name().is_not_in(["1"])
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([
            ("1", "2"), ("2", "3"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page")
        ])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edge_for_src_dst():
    def check(graph):
        filter_expr1 = filter.Edge.src().name() == "3"
        filter_expr2 = filter.Edge.dst().name() == "1"
        result_ids = sorted(graph.filter_edges(filter_expr1 & filter_expr2).edges.id)
        expected_ids = [("3", "1")]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_contains():
    def check(graph):
        filter_expr = filter.Edge.src().name().contains("Mayer")
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([('John Mayer', 'Jimmy Page')])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_src_not_contains():
    def check(graph):
        filter_expr = filter.Edge.src().name().not_contains("Mayer")
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([
            ("1", "2"), ("2", "1"), ("2", "3"), ("3", "1"),
            ("David Gilmour", "John Mayer")
        ])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_fuzzy_search():
    def check(graph):
        filter_expr = filter.Edge.src().name().fuzzy_search("John", 2, True)
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = [("John Mayer", "Jimmy Page")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.src().name().fuzzy_search("John", 2, False)
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Edge.dst().name().fuzzy_search("John May", 2, False)
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = [('David Gilmour', 'John Mayer')]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_not_src():
    def check(graph):
        filter_expr = filter.Edge.src().name().not_contains("Mayer")
        result_ids = sorted(graph.filter_edges(~filter_expr).edges.id)
        expected_ids = [('John Mayer', 'Jimmy Page')]
        assert result_ids == expected_ids

    return check

