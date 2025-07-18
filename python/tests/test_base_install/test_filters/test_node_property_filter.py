from raphtory import filter
from filters_setup import init_graph
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

        filter_expr = filter.Node.property("p10").constant().contains("Paper")
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

        filter_expr = filter.Node.property("p10").temporal().latest().not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr = filter.Node.property("p10").constant().not_contains("ship")
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
