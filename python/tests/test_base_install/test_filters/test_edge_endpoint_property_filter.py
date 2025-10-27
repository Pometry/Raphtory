from raphtory import filter
from filters_setup import init_graph, create_test_graph
from utils import with_disk_variants
import pytest


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_src_property_eq():
    def check(graph):
        expr = filter.Edge.src().property("p2") == 2
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "1"), ("2", "3")])
        assert result == expected
    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_dst_property_contains():
    def check(graph):
        # dst node "3" has p20 == "Gold_boat"; dst node "4" has p20 updated from Gold_boat to Gold_ship so it doesn't appear here
        expr = filter.Edge.dst().property("p20").contains("boat")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected
    return check

@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_dst_property_any_contains():
    def check(graph):
        # dst node "3" has p20 == "Gold_boat"; dst node "4" has p20 updated from Gold_boat to Gold_ship so it appears with .temporal().any()
        expr = filter.Edge.dst().property("p20").temporal().any().contains("boat")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3"), ("3", "4")])
        assert result == expected
    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_dst_property_gt():
    def check(graph):
        expr = filter.Edge.dst().property("p100") > 55
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])  # there are edges (2,3) at times 2 and 3; IDs dedup by view
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_property_temporal_sum():
    def check(graph):
        expr = filter.Edge.src().property("prop6").temporal().last().sum() == 12
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_property_any_equals():
    def check(graph):
        # src node "d" doesn't exist as src; src of edges are a,b,c; node "a" has prop8=[2,3,3]
        expr = filter.Edge.src().property("prop8").temporal().any().any() == 3
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_metadata_sum():
    def check(graph):
        # src node "a" metadata prop1 sum == 36
        # src node "b" and "c" property prop1 < 36, shouldn't appear because it's property not metadata
        expr = filter.Edge.src().metadata("prop1").sum() <= 36
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_metadata_avg():
    def check(graph):
        expr = filter.Edge.src().metadata("prop2").avg() <= 2.0
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d"), ("b", "d")])
        assert result == expected
    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_src_property_and_edge_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) & (
            filter.Edge.property("p20").temporal().any().contains("ship")
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected
    return check

@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_src_and_dst_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) & (
            filter.Edge.dst().property("p20").contains("boat")
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected
    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_src_or_dst_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) | (
            filter.Edge.dst().property("p20").contains("boat")
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "1"), ("2", "3")])
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_property_and_dst_name():
    def check(graph):
        expr = (filter.Edge.src().property("prop1") >= 20) & (
                filter.Edge.dst().name() == "d"
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d"), ("c", "d")])
        assert result == expected
    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_edges_src_metadata_and_edge_property():
    def check(graph):
        expr = (filter.Edge.src().metadata("prop1").sum() == 36) & (
                filter.Edge.property("eprop1") > 20
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected
    return check