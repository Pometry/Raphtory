from raphtory import Prop, filter
import pytest
from test_base_install.test_filters.conftest import generate_graph_variants

graph, persistent_graph, event_disk_graph, persistent_disk_graph = generate_graph_variants()


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_name_eq(graph):
    filter_expr = filter.Node.name() == "3"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["3"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_name_ne(graph):
    filter_expr = filter.Node.name() != "2"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3", "4", "David Gilmour",  "Jimmy Page", "John Mayer"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_name_in(graph):
    filter_expr = filter.Node.name().is_in(["1"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1"]
    assert result_ids == expected_ids

    filter_expr = filter.Node.name().is_in(["2", "3"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2", "3"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_name_not_in(graph):
    filter_expr = filter.Node.name().is_not_in(["1"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2", "3", "4", "David Gilmour",  "Jimmy Page", "John Mayer"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_eq(graph):
    filter_expr = filter.Node.node_type() == "fire_nation"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_ne(graph):
    filter_expr = filter.Node.node_type() != "fire_nation"
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2", "4", "David Gilmour",  "Jimmy Page", "John Mayer"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_in(graph):
    filter_expr = filter.Node.node_type().is_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().is_in(["fire_nation", "air_nomads"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "2", "3"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_not_in(graph):
    filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2", "4", "David Gilmour",  "Jimmy Page", "John Mayer"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_contains(graph):
    filter_expr = filter.Node.node_type().contains("fire")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_node_type_not_contains(graph):
    filter_expr = filter.Node.node_type().not_contains("fire")
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2", "4", "David Gilmour",  "Jimmy Page", "John Mayer"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_fuzzy_search(graph):
    filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, True)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, False)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = []
    assert result_ids == expected_ids

    filter_expr = filter.Node.node_type().fuzzy_search("air_noma", 2, False)
    result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)
    expected_ids = ["2"]
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, persistent_graph, event_disk_graph, persistent_disk_graph])
def test_filter_nodes_for_not_node_type(graph):
    filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
    result_ids = sorted(graph.filter_nodes(~filter_expr).nodes.id)
    expected_ids = ["1", "3"]
    assert result_ids == expected_ids
