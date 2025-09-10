from raphtory import filter, Prop
from filters_setup import init_graph, create_test_graph, create_test_graph2
from utils import with_disk_variants


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_eq():
    def check(graph):
        filter_expr = filter.Node.property("p2") == 2
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_ne():
    def check(graph):
        filter_expr = filter.Node.property("p2") != 2
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_lt():
    def check(graph):
        filter_expr = filter.Node.property("p2") < 10
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_le():
    def check(graph):
        filter_expr = filter.Node.property("p2") <= 6
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_gt():
    def check(graph):
        filter_expr = filter.Node.property("p2") > 2
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_nodes_for_property_ge():
    def check(graph):
        filter_expr = filter.Node.property("p2") >= 2
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_in():
    def check(graph):
        filter_expr = filter.Node.property("p2").is_in([6])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p2").is_in([2, 6])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_not_in():
    def check(graph):
        filter_expr = filter.Node.property("p2").is_not_in([6])
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_is_some():
    def check(graph):
        filter_expr = filter.Node.property("p2").is_some()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_is_none():
    def check(graph):
        filter_expr = filter.Node.property("p2").is_none()
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_by_props_added_at_different_times():
    def check(graph):
        filter_expr1 = filter.Node.property("p4") == "pometry"
        filter_expr2 = filter.Node.property("p5") == 12
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["4"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_starts_with():
    def check(graph):
        filter_expr = filter.Node.property("p10").starts_with("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().any().starts_with("Pap")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().any().starts_with("Cap")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().latest().starts_with("P")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.metadata("p10").starts_with("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p20").temporal().first().starts_with("Old")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p20").temporal().all().starts_with("Gold")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3", "4"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_ends_with():
    def check(graph):
        filter_expr = filter.Node.property("p10").ends_with("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").ends_with("clip")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().any().ends_with("lane")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().latest().ends_with("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p20").temporal().first().ends_with("boat")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3", "4"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p20").temporal().all().ends_with("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.metadata("p10").ends_with("ane")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_contains():
    def check(graph):
        filter_expr = filter.Node.property("p10").contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().any().contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().latest().contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "2", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p20").temporal().first().contains("boat")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3", "4"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.metadata("p10").contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_property_not_contains():
    def check(graph):
        filter_expr = filter.Node.property("p10").not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").temporal().any().not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = (
            filter.Node.property("p10").temporal().latest().not_contains("ship")
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = (
            filter.Node.property("p20").temporal().first().not_contains("ship")
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["3", "4"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.metadata("p10").not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_filter_nodes_for_not_property():
    def check(graph):
        filter_expr = filter.Node.property("p2") > 2
        result_ids = sorted(graph.filter(~filter_expr).nodes.id)
        expected_ids = ["1", "2", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_property_sum():
    def check(graph):
        # Since this graph is created in python prop5 values are all i64
        filter_expr = filter.Node.property("prop5").sum() == Prop.i64(6)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_property_avg():
    def check(graph):
        filter_expr = filter.Node.property("prop5").avg() == 2.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_property_min():
    def check(graph):
        filter_expr = filter.Node.property("prop5").min() == 1
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_property_max():
    def check(graph):
        filter_expr = filter.Node.property("prop5").max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_property_len():
    def check(graph):
        filter_expr = filter.Node.property("prop7").len() == Prop.u64(3)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["c"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_latest_property_sum():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().latest().sum() == 12
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_latest_property_avg():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().latest().avg() == 4.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_latest_property_min():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().latest().min() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_latest_property_max():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().latest().max() == 5
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_latest_property_len():
    def check(graph):
        filter_expr = filter.Node.property(
            "prop6"
        ).temporal().latest().len() == Prop.u64(3)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_any_property_sum():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().any().sum() == 12
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().any().sum() == 6
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_any_property_avg():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().any().avg() == 2.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().any().avg() == 4.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_any_property_min():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().any().min() == 1
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().any().min() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_any_property_max():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().any().max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().any().max() == 5
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_any_property_len():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().any().len() == Prop.u64(
            3
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_all_property_sum():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().all().sum() == 6
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().all().sum() == 6
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_all_property_avg():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().all().avg() == 2.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().all().avg() == 2.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_all_property_min():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().all().min() == 1
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().all().min() == 1
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_all_property_max():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().all().max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("prop6").temporal().all().max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_all_property_len():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().all().len() == Prop.u64(
            3
        )
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a", "c"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_first_property_sum():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().first().sum() == 6
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_first_property_avg():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().first().avg() == 2.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_first_property_min():
    def check(graph):
        filter_expr = filter.Node.property("prop5").temporal().first().min() == 1
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_first_property_max():
    def check(graph):
        filter_expr = filter.Node.property("prop6").temporal().first().max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_temporal_first_property_len():
    def check(graph):
        filter_expr = filter.Node.property(
            "prop6"
        ).temporal().first().len() == Prop.u64(3)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_metadata_sum():
    def check(graph):
        filter_expr = filter.Node.metadata("prop4").sum() == 23
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["b"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_metadata_avg():
    def check(graph):
        filter_expr = filter.Node.metadata("prop1").avg() == 12.0
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_metadata_min():
    def check(graph):
        filter_expr = filter.Node.metadata("prop2").min() == -2
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a", "b"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_metadata_max():
    def check(graph):
        filter_expr = filter.Node.metadata("prop2").max() == 3
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["a"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=("graph", "persistent_graph"))
def test_filter_nodes_for_metadata_len():
    def check(graph):
        filter_expr = filter.Node.metadata("prop4").len() == Prop.u64(2)
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["b"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_nodes_getitem_property_filter_expr():
    def check(graph):
        # Test 1
        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.nodes[filter_expr].id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.nodes[filter_expr].neighbours.name.collect())
        expected_ids = [["1", "2", "4"], ["2", "3"]]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.filter(filter_expr).nodes.neighbours.name.collect())
        expected_ids = [
            ["1"],
            ["3"],
        ]  # graph filter applies to nodes neighbours as well
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.nodes[filter_expr].degree())
        expected_ids = [2, 3]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p100") > 30
        result_ids = sorted(graph.filter(filter_expr).nodes.degree())
        expected_ids = [1, 1]  # graph filter applies to nodes neighbours as well
        assert result_ids == expected_ids

        # Test 2
        filter_expr2 = filter.Node.property("p9") == 5
        result_ids = graph.nodes[filter_expr][filter_expr2].id.collect()
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.nodes[filter_expr3].id.collect()
        expected_ids = ["1"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_path_from_graph_nodes_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Node.property("p100") > 30

        # Test 1
        result_ids = graph.nodes.id.collect()
        expected_ids = ["1", "2", "3", "4", "David Gilmour", "John Mayer", "Jimmy Page"]
        assert result_ids == expected_ids

        result_ids = graph.nodes.neighbours.id.collect()
        expected_ids = [
            ["2", "3"],
            ["1", "3"],
            ["1", "2", "4"],
            ["3"],
            ["John Mayer"],
            ["David Gilmour", "Jimmy Page"],
            ["John Mayer"],
        ]
        assert result_ids == expected_ids

        result_ids = graph.nodes.neighbours[filter_expr].id.collect()
        expected_ids = [["3"], ["1", "3"], ["1"], ["3"], [], [], []]
        assert result_ids == expected_ids

        result_ids = graph.nodes.neighbours[filter_expr].neighbours.id.collect()
        expected_ids = [
            ["1", "2", "4"],
            ["2", "3", "1", "2", "4"],
            ["2", "3"],
            ["1", "2", "4"],
            [],
            [],
            [],
        ]
        assert result_ids == expected_ids

        # Test 2
        filter_expr2 = filter.Node.property("p9") == 5
        result_ids = graph.nodes.neighbours[filter_expr][filter_expr2].id.collect()
        expected_ids = [[], ["1"], ["1"], [], [], [], []]
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.nodes.neighbours[filter_expr3].id.collect()
        expected_ids = [[], ["1"], ["1"], [], [], [], []]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_path_from_node_nodes_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Node.property("p100") > 30
        assert graph.node("1") is not None

        # Test 1
        result_ids = graph.node("1").neighbours.id.collect()
        expected_ids = ["2", "3"]
        assert result_ids == expected_ids

        result_ids = graph.node("1").neighbours[filter_expr].id.collect()
        expected_ids = ["3"]
        assert result_ids == expected_ids

        result_ids = graph.node("1").neighbours[filter_expr].neighbours.id.collect()
        expected_ids = ["1", "2", "4"]
        assert result_ids == expected_ids

        # Test 2
        filter_expr2 = filter.Node.property("p3") == 1
        result_ids = graph.node("1").neighbours[filter_expr][filter_expr2].id.collect()
        expected_ids = ["3"]
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.node("1").neighbours[filter_expr3].id.collect()
        expected_ids = ["3"]
        assert result_ids == expected_ids

    return check
