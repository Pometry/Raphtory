from raphtory import filter
import pytest
from filters_setup import (
    init_nodes_graph,
    init_nodes_graph1,
    init_nodes_graph2,
    combined,
)
from utils import with_disk_variants


def init_graph_for_secondary_indexes(graph):
    graph.add_node(1, "N16", {"p1": 2})
    graph.add_node(1, "N16", {"p1": 1})

    graph.add_node(1, "N17", {"p1": 1})
    graph.add_node(1, "N17", {"p1": 2})

    return graph


@with_disk_variants(init_nodes_graph)
def test_metadata_semantics():
    def check(graph):
        filter_expr = filter.Metadata("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph)
def test_temporal_any_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().any() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_nodes_graph, init_graph_for_secondary_indexes]),
)
def test_temporal_any_semantics_for_secondary_indexes():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().any() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(
            ["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph)
def test_temporal_latest_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_nodes_graph, init_graph_for_secondary_indexes]),
)
def test_temporal_latest_semantics_for_secondary_indexes():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N16", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph)
def test_property_semantics():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N3", "N4", "N6", "N7"])
        print(list(zip(graph.nodes.id, graph.nodes.properties.get("p1"))))
        assert result_ids == expected_ids

    return check


@with_disk_variants(
    init_fn=combined([init_nodes_graph, init_graph_for_secondary_indexes]),
)
def test_property_semantics_for_secondary_indexes():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N16", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph1)
def test_property_semantics_only_metadata():
    def check(graph):
        filter_expr = filter.Metadata("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N2"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph2)
def test_property_semantics_only_temporal():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N3"])
        assert result_ids == expected_ids

    return check
