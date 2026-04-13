from raphtory import filter
from filters_setup import init_graph, init_graph2, create_test_graph
from utils import with_disk_variants
import pytest


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_name_eq():
    def check(graph):
        filter_expr = filter.Node.name() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_name_ne():
    def check(graph):
        filter_expr = filter.Node.name() != "2"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_name_in():
    def check(graph):
        filter_expr = filter.Node.name().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.name().is_in(["2", "3"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_name_not_in():
    def check(graph):
        filter_expr = filter.Node.name().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_eq():
    def check(graph):
        filter_expr = filter.Node.node_type() == "fire_nation"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_ne():
    def check(graph):
        filter_expr = filter.Node.node_type() != "fire_nation"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_in():
    def check(graph):
        filter_expr = filter.Node.node_type().is_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().is_in(["fire_nation", "air_nomads"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_not_in():
    def check(graph):
        filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_starts_with():
    def check(graph):
        filter_expr = filter.Node.node_type().starts_with("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().starts_with("Liar")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_ends_with():
    def check(graph):
        filter_expr = filter.Node.node_type().ends_with("tion")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().ends_with("station")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_contains():
    def check(graph):
        filter_expr = filter.Node.node_type().contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_node_type_not_contains():
    def check(graph):
        filter_expr = filter.Node.node_type().not_contains("fire")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_fuzzy_search():
    def check(graph):
        filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, True)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().fuzzy_search("fire", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node.node_type().fuzzy_search("air_noma", 2, False)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_not_node_type():
    def check(graph):
        filter_expr = filter.Node.node_type().is_not_in(["fire_nation"])
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node.id() == "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_eq():
    def check(graph):
        filter_expr = filter.Node.id() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [3]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node.id() != "3"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_ne():
    def check(graph):
        filter_expr = filter.Node.id() != 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1, 2, 4]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node.id().is_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_in():
    def check(graph):
        filter_expr = filter.Node.id().is_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [1]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_with_str_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node.id().is_not_in(["1"])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_with_num_ids_for_node_id_is_not_in():
    def check(graph):
        filter_expr = filter.Node.id().is_not_in([1])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = [2, 3, 4]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_with_str_ids_error():
    def check(graph):
        filter_expr = filter.Node.id() == 3
        with pytest.raises(
            Exception,
            match='Invalid filter: Filter value type does not match node ID type. Expected Str but got "U64"',
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_with_num_ids_error():
    def check(graph):
        filter_expr = filter.Node.id() == "3"
        with pytest.raises(
            Exception,
            match='Invalid filter: Filter value type does not match node ID type. Expected U64 but got "Str"',
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_filter_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node.is_active()
        result_ids = sorted(graph.window(1, 4).filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_select_nodes_is_active():
    def check(graph):
        filter_expr = filter.Node.is_active()
        result_ids = sorted(graph.window(1, 4).nodes[filter_expr].id)
        expected_ids = sorted(["1", "2", "3", "4"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_filter_nodes_windowed_is_active():
    def check(graph):
        filter_expr = filter.Node.window(1, 2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=["graph", "persistent_graph"])
def test_filter_nodes_windowed_is_active_not():
    def check(graph):
        filter_expr = filter.Node.window(1, 2).is_active()
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = sorted([])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_filter_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_select_nodes_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.latest().is_active()
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_filter_nodes_snapshot_latest_is_active():
    def check(graph):
        filter_expr = filter.Node.snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(
            ["1", "2", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["persistent_graph"])
def test_filter_nodes_snapshot_latest_is_active_persistent():
    def check(graph):
        filter_expr = filter.Node.snapshot_latest().is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_filter_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node.at(2).is_active()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted(["1", "2", "3"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "persistent_graph"])
def test_select_nodes_at_is_active():
    def check(graph):
        filter_expr = filter.Node.at(2).is_active()
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted(["1", "2", "3"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph2)
def test_filter_nodes_neighbours():
    def check(graph):
        filter_expr = filter.Graph.window(1, 5)
        result_ids = sorted(graph.node(1).neighbours[filter_expr].id)
        expected_ids = [2, 3]
        assert result_ids == expected_ids

    return check


def test_filter_nodes_by_column():
    from raphtory import Graph
    from raphtory.algorithms import alternating_mask

    graph = Graph()
    graph.add_node(1, 1, {})
    graph.add_node(1, 2, {})
    graph.add_node(1, 3, {})
    graph.add_node(1, 4, {})
    graph.add_node(1, 5, {})

    actual = alternating_mask(graph)
    expected = {
        1: {"bool_col": False},
        2: {"bool_col": True},
        3: {"bool_col": False},
        4: {"bool_col": True},
        5: {"bool_col": False},
    }
    assert actual == expected

    filter_expr = filter.Node.by_state_column(actual, "bool_col")
    result_ids = sorted(graph.filter(filter_expr).nodes.id)
    expected_ids = sorted([2, 4])
    assert result_ids == expected_ids

    result_ids = sorted(graph.nodes[filter_expr].id)
    expected_ids = sorted([2, 4])
    assert result_ids == expected_ids
