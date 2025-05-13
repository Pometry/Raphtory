from raphtory import filter, Prop
import pytest
from test_base_install.test_filters.conftest import generate_graph_variants
from test_base_install.test_filters.semantics.conftest import init_edges_graph, init_edges_graph1, init_edges_graph2

graph, persistent_graph, event_disk_graph, persistent_disk_graph = generate_graph_variants(init_edges_graph)

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
@pytest.mark.parametrize("graph", [graph])
def test_constant_semantics(graph):
    filter_expr = filter.Property("p1").constant() == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N10","N11"), ("N11","N12"), ("N12","N13"), ("N13","N14"), ("N14","N15"), ("N15","N1"), ("N9","N10")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_temporal_any_semantics(graph):
    filter_expr = filter.Property("p1").temporal().any() == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N2","N3"), ("N3","N4"), ("N4","N5"), ("N5","N6"), ("N6","N7"), ("N7","N8"),("N8","N9")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("base_graph", [graph])
def test_temporal_any_semantics_for_secondary_indexes(base_graph):
    # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
    graph = type(base_graph)()
    graph = init_edges_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().any() == 1

    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N16","N15"), ("N17","N16"), ("N2","N3"), ("N3","N4"), ("N4","N5"), ("N5","N6"), ("N6","N7"), ("N7","N8"), ("N8","N9")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [event_disk_graph])
def test_temporal_any_semantics_for_secondary_indexes_dsg(graph):
    with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
        graph = init_graph_for_secondary_indexes(graph)
        filter_expr = filter.Property("p1").temporal().any() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)


@pytest.mark.parametrize("graph", [graph, event_disk_graph])
def test_temporal_latest_semantics(graph):
    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N3","N4"), ("N4","N5"), ("N6","N7"), ("N7","N8")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("base_graph", [graph])
def test_temporal_latest_semantics_for_secondary_indexes(base_graph):
    # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
    graph = type(base_graph)()
    graph = init_edges_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1").temporal().latest() == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N16","N15"), ("N3","N4"), ("N4","N5"), ("N6","N7"), ("N7","N8")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [event_disk_graph])
def test_temporal_latest_semantics_for_secondary_indexes_dsg(graph):
    with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
        graph = init_graph_for_secondary_indexes(graph)
        filter_expr = filter.Property("p1").temporal().latest() == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)


@pytest.mark.parametrize("graph", [graph])
def test_property_semantics(graph):
    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N14","N15"), ("N15","N1"), ("N3","N4"), ("N4","N5"), ("N6","N7"), ("N7","N8")])
    assert result_ids == expected_ids


# Disk graph doesn't have constant edge properties
@pytest.mark.parametrize("graph", [event_disk_graph])
def test_property_semantics2(graph):
    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N3","N4"), ("N4","N5"), ("N6","N7"), ("N7","N8")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("base_graph", [graph])
def test_property_semantics_for_secondary_indexes(base_graph):
    # Create a new graph using the same type as base_graph (Graph or PersistentGraph)
    graph = type(base_graph)()
    graph = init_edges_graph(graph)
    graph = init_graph_for_secondary_indexes(graph)

    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N14","N15"), ("N15","N1"), ("N16","N15"), ("N3","N4"), ("N4","N5"), ("N6","N7"), ("N7","N8")])
    assert result_ids == expected_ids


@pytest.mark.parametrize("graph", [event_disk_graph])
def test_property_semantics_for_secondary_indexes_dsg(graph):
    with pytest.raises(Exception, match="Immutable graph is .. immutable!"):
        graph = init_graph_for_secondary_indexes(graph)
        filter_expr = filter.Property("p1") == 1
        result_ids = sorted(graph.filter_nodes(filter_expr).nodes.id)


graph1, persistent_graph1, event_disk_graph1, persistent_disk_graph1 = generate_graph_variants(init_edges_graph1)

@pytest.mark.parametrize("graph", [graph1])
def test_property_semantics_only_constant(graph):
    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N2","N3")])
    assert result_ids == expected_ids

# Disk graph doesn't have constant edge properties
@pytest.mark.parametrize("graph", [event_disk_graph1])
def test_property_semantics_only_constant2(graph):
    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = []
    assert result_ids == expected_ids


graph2, persistent_graph2, event_disk_graph2, persistent_disk_graph2 = generate_graph_variants(init_edges_graph2)

@pytest.mark.parametrize("graph", [graph2, event_disk_graph2])
def test_property_semantics_only_temporal(graph):
    filter_expr = filter.Property("p1") == 1
    result_ids = sorted(graph.filter_edges(filter_expr).edges.id)
    expected_ids = sorted([("N1","N2"), ("N3","N4")])
    assert result_ids == expected_ids
