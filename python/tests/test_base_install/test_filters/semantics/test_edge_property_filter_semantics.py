from raphtory import filter
import pytest
from filters_setup import (
    init_edges_graph,
    init_edges_graph1,
    init_edges_graph2,
    combined,
)
from utils import with_disk_variants

# TODO: PropertyFilteringNotImplemented for variants persistent_graph for filter_edges.


def init_graph_for_secondary_indexes(graph):
    edges = [
        (1, "N16", "N15", {"p1": 2}),
        (1, "N16", "N15", {"p1": 1}),
        (1, "N17", "N16", {"p1": 1}),
        (1, "N17", "N16", {"p1": 2}),
    ]

    for time, src, dst, props in edges:
        graph.add_edge(time, src, dst, props)

    return graph


# Disk graph doesn't have constant edge properties
@with_disk_variants(init_edges_graph, variants=["graph"])
def test_metadata_semantics():
    def check(graph):
        filter_expr = filter.Metadata("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N10", "N11"),
                ("N11", "N12"),
                ("N12", "N13"),
                ("N13", "N14"),
                ("N14", "N15"),
                ("N15", "N1"),
                ("N9", "N10"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_edges_graph, variants=["graph", "event_disk_graph"])
def test_temporal_any_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().any() == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N2", "N3"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N5", "N6"),
                ("N6", "N7"),
                ("N7", "N8"),
                ("N8", "N9"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_edges_graph, init_graph_for_secondary_indexes]),
    variants=["graph", "event_disk_graph"],
)
def test_temporal_any_semantics_for_secondary_indexes():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().any() == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N16", "N15"),
                ("N17", "N16"),
                ("N2", "N3"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N5", "N6"),
                ("N6", "N7"),
                ("N7", "N8"),
                ("N8", "N9"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_edges_graph, variants=["graph", "event_disk_graph"])
def test_temporal_latest_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [("N1", "N2"), ("N3", "N4"), ("N4", "N5"), ("N6", "N7"), ("N7", "N8")]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_edges_graph, init_graph_for_secondary_indexes]),
    variants=["graph", "event_disk_graph"],
)
def test_temporal_latest_semantics_for_secondary_indexes3():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N16", "N15"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N6", "N7"),
                ("N7", "N8"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_edges_graph, variants=["graph"])
def test_property_semantics():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N6", "N7"),
                ("N7", "N8"),
            ]
        )
        assert result_ids == expected_ids

    return check


# Disk graph doesn't have constant edge properties
@with_disk_variants(init_edges_graph, variants=["event_disk_graph"])
def test_property_semantics2():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [("N1", "N2"), ("N3", "N4"), ("N4", "N5"), ("N6", "N7"), ("N7", "N8")]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_edges_graph, init_graph_for_secondary_indexes]),
    variants=["graph"],
)
def test_property_semantics_for_secondary_indexes():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N16", "N15"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N6", "N7"),
                ("N7", "N8"),
            ]
        )
        assert result_ids == expected_ids

    return check


# TODO: Const properties not supported for disk_graph.
@with_disk_variants(
    init_fn=combined([init_edges_graph, init_graph_for_secondary_indexes]),
    variants=["event_disk_graph"],
)
def test_property_semantics_for_secondary_indexes_dsg():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("N1", "N2"),
                ("N16", "N15"),
                ("N3", "N4"),
                ("N4", "N5"),
                ("N6", "N7"),
                ("N7", "N8"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_edges_graph1, variants=["graph"])
def test_property_semantics_only_metadata():
    def check(graph):
        filter_expr = filter.Metadata("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("N1", "N2"), ("N2", "N3")])
        assert result_ids == expected_ids

    return check


# Disk graph doesn't have constant edge properties
@with_disk_variants(init_edges_graph1, variants=["event_disk_graph"])
def test_property_semantics_only_metadata2():
    def check(graph):
        filter_expr = filter.Metadata("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_edges_graph2, variants=["graph", "event_disk_graph"])
def test_property_semantics_only_temporal():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
        expected_ids = sorted([("N1", "N2"), ("N3", "N4")])
        assert result_ids == expected_ids

    return check
