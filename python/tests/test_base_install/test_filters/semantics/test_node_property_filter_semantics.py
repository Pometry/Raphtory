from raphtory import filter, Prop
import pytest
from .conftest import init_nodes_graph, init_nodes_graph1, init_nodes_graph2
from utils import with_disk_variants


def init_graph_for_secondary_indexes(graph):
    graph.add_node(1, "N16", {"p1": 2})
    graph.add_node(1, "N16", {"p1": 1})

    graph.add_node(1, "N17", {"p1": 1})
    graph.add_node(1, "N17", {"p1": 2})

    return graph


@with_disk_variants(init_nodes_graph)
def test_constant_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").constant() == 1
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


@with_disk_variants(init_nodes_graph, variants=["graph", "persistent_disk_graph"])
def test_temporal_any_semantics_for_secondary_indexes():
    def check(base_graph):
        # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
        graph = type(base_graph)()
        graph = init_nodes_graph(graph)
        graph = init_graph_for_secondary_indexes(graph)

        filter_expr = filter.Property("p1").temporal().any() == 1

        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph, variants=["event_disk_graph", "persistent_disk_graph"])
def test_temporal_any_semantics_for_secondary_indexes_dsg():
    def check(graph):
        with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
            graph = init_graph_for_secondary_indexes(graph)
            filter_expr = filter.Property("p1").temporal().any() == 1
            result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)

    return check


@with_disk_variants(init_nodes_graph)
def test_temporal_latest_semantics():
    def check(graph):
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph, variants=["graph", "persistent_disk_graph"])
def test_temporal_latest_semantics_for_secondary_indexes():
    def check(base_graph):
        # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
        graph = type(base_graph)()
        graph = init_nodes_graph(graph)
        graph = init_graph_for_secondary_indexes(graph)

        filter_expr = filter.Property("p1").temporal().latest() == 1

        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N16", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph, variants=["event_disk_graph", "persistent_disk_graph"])
def test_temporal_latest_semantics_for_secondary_indexes_dsg():
    def check(graph):
        with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
            graph = init_graph_for_secondary_indexes(graph)
            filter_expr = filter.Property("p1").temporal().latest() == 1
            result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)

    return check


@with_disk_variants(init_nodes_graph)
def test_property_semantics():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N14", "N15", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph, variants=["graph", "persistent_disk_graph"])
def test_property_semantics_for_secondary_indexes():
    def check(base_graph):
        # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
        graph = type(base_graph)()
        graph = init_nodes_graph(graph)
        graph = init_graph_for_secondary_indexes(graph)

        filter_expr = filter.Property("p1") == 1

        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
        expected_ids = sorted(["N1", "N14", "N15", "N16", "N3", "N4", "N6", "N7"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_nodes_graph, variants=["event_disk_graph", "persistent_disk_graph"])
def test_property_semantics_for_secondary_indexes_dsg():
    def check(graph):
        with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
            graph = init_graph_for_secondary_indexes(graph)
            filter_expr = filter.Property("p1") == 1
            result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)

    return check


@with_disk_variants(init_nodes_graph1)
def test_property_semantics_only_constant():
    def check(graph):
        filter_expr = filter.Property("p1") == 1
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
