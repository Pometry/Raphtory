from raphtory import filter, Prop
from filters_setup import init_graph, init_graph2, init_graph_degree_filter, create_test_graph
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


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_out_degree_eq():
    def check(graph):
        # Nodes with out_degree == 2
        filter_expr = filter.Node.degree("out") == Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() == 2])
        assert result_ids == expected_ids

        # Nodes with out_degree == 1
        filter_expr = filter.Node.degree("out") == Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() == 1])
        assert result_ids == expected_ids

        # Nodes with out_degree == 0
        filter_expr = filter.Node.degree("out") == Prop.u64(0)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() == 0])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_in_degree_eq():
    def check(graph):
        # Nodes with in_degree == 2
        filter_expr = filter.Node.degree("in") == Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() == 2])
        assert result_ids == expected_ids

        # Nodes with in_degree == 1
        filter_expr = filter.Node.degree("in") == Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() == 1])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_both_eq():
    def check(graph):
        # Nodes with degree(BOTH) == 1
        filter_expr = filter.Node.degree("both") == Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() == 1])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) == 2
        filter_expr = filter.Node.degree("both") == Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() == 2])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) == 3
        filter_expr = filter.Node.degree("both") == Prop.u64(3)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() == 3])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_ne():
    def check(graph):
        # Nodes with out_degree != 0
        filter_expr = filter.Node.degree("out") != Prop.u64(0)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() != 0])
        assert result_ids == expected_ids

        # Nodes with in_degree != 2
        filter_expr = filter.Node.degree("in") != Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() != 2])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_gt():
    def check(graph):
        # Nodes with out_degree > 0
        filter_expr = filter.Node.degree("out") > Prop.u64(0)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() > 0])
        assert result_ids == expected_ids

        # Nodes with out_degree > 1
        filter_expr = filter.Node.degree("out") > Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() > 1])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) > 2
        filter_expr = filter.Node.degree("both") > Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() > 2])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_ge():
    def check(graph):
        # Nodes with out_degree >= 2
        filter_expr = filter.Node.degree("out") >= Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() >= 2])
        assert result_ids == expected_ids

        # Nodes with in_degree >= 1
        filter_expr = filter.Node.degree("in") >= Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() >= 1])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) >= 2
        filter_expr = filter.Node.degree("both") >= Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() >= 2])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_lt():
    def check(graph):
        # Nodes with out_degree < 2
        filter_expr = filter.Node.degree("out") < Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() < 2])
        assert result_ids == expected_ids

        # Nodes with in_degree < 2
        filter_expr = filter.Node.degree("in") < Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() < 2])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) < 2
        filter_expr = filter.Node.degree("both") < Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() < 2])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_le():
    def check(graph):
        # Nodes with out_degree <= 1
        filter_expr = filter.Node.degree("out") <= Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() <= 1])
        assert result_ids == expected_ids

        # Nodes with in_degree <= 1
        filter_expr = filter.Node.degree("in") <= Prop.u64(1)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() <= 1])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) <= 2
        filter_expr = filter.Node.degree("both") <= Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() <= 2])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_is_in():
    def check(graph):
        # Nodes with out_degree in [0, 1]
        filter_expr = filter.Node.degree("out").is_in([Prop.u64(0), Prop.u64(1)])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() in [0, 1]])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) in [1, 3]
        filter_expr = filter.Node.degree("both").is_in([Prop.u64(1), Prop.u64(3)])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() in [1, 3]])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_is_not_in():
    def check(graph):
        # Nodes with out_degree not in [0]
        filter_expr = filter.Node.degree("out").is_not_in([Prop.u64(0)])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() not in [0]])
        assert result_ids == expected_ids

        # Nodes with degree(BOTH) not in [1, 2]
        filter_expr = filter.Node.degree("both").is_not_in([Prop.u64(1), Prop.u64(2)])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.degree() not in [1, 2]])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_combined_filters():
    def check(graph):
        # Nodes with out_degree > 0 AND in_degree > 0
        filter_expr = (filter.Node.degree("out") > Prop.u64(0)) & (
            filter.Node.degree("in") > Prop.u64(0)
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() > 0 and n.in_degree() > 0])
        assert result_ids == expected_ids

        # Nodes with out_degree == 0 OR in_degree == 2
        filter_expr = (filter.Node.degree("out") == Prop.u64(0)) | (
            filter.Node.degree("in") == Prop.u64(2)
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() == 0 or n.in_degree() == 2])
        assert result_ids == expected_ids

        # Nodes with out_degree >= 1 AND node_type == "test_type"
        filter_expr = (filter.Node.degree("out") >= Prop.u64(1)) & (
            filter.Node.node_type() == "test_type"
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() >= 1 and n.node_type == "test_type"])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_negation():
    def check(graph):
        # NOT (out_degree == 2) -> nodes with out_degree != 2
        filter_expr = ~(filter.Node.degree("out") == Prop.u64(2))
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if not (n.out_degree() == 2)])
        assert result_ids == expected_ids

        # NOT (degree(BOTH) < 2) -> nodes with degree >= 2
        filter_expr = ~(filter.Node.degree("both") < Prop.u64(2))
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = sorted([n.id for n in graph.nodes if not (n.degree() < 2)])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph_degree_filter)
def test_filter_nodes_by_degree_using_nodes_accessor():
    def check(graph):
        # Test using graph.nodes[filter_expr] syntax
        filter_expr = filter.Node.degree("out") >= Prop.u64(2)
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted([n.id for n in graph.nodes if n.out_degree() >= 2])
        assert result_ids == expected_ids

        # Test with in_degree
        filter_expr = filter.Node.degree("in") == Prop.u64(1)
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = sorted([n.id for n in graph.nodes if n.in_degree() == 1])
        assert result_ids == expected_ids

    return check
