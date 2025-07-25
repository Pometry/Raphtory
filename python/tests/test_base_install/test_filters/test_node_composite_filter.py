from raphtory import filter
from filters_setup import init_graph, create_test_graph
from utils import with_disk_variants


@with_disk_variants(init_graph)
def test_node_composite_filter():
    def check(graph):
        filter_expr1 = filter.Node.property("p2") == 2
        filter_expr2 = filter.Node.property("p1") == "kapoor"
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.property("p2") > 2
        filter_expr2 = filter.Node.property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter(filter_expr1 | filter_expr2).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.property("p9") < 9
        filter_expr2 = filter.Node.property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.node_type() == "fire_nation"
        filter_expr2 = filter.Node.property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Node.property("p2") >= 2
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Node.property("p2") == 2
        filter_expr3 = filter.Node.property("p9") <= 5
        result_ids = sorted(
            graph.filter((filter_expr1 & filter_expr2) | filter_expr3).nodes.id
        )
        expected_ids = ["1", "2"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_not_node_composite_filter():
    def check(graph):
        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Node.property("p2") >= 2
        result_ids = sorted(graph.filter(~filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

        result_ids = sorted(graph.filter(~(filter_expr1 & filter_expr2)).nodes.id)
        expected_ids = sorted(
            ["1", "3", "4", "David Gilmour", "Jimmy Page", "John Mayer"]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(create_test_graph, variants=["graph", "persistent_graph"])
def test_out_neighbours_composite_filter():
    def check(graph):
        filter_expr1 = filter.Node.name() == "d"
        filter_expr2 = filter.Node.property("prop1") > 10
        result_names = sorted(
            graph.node("a")
            .filter(filter_expr1 & filter_expr2)
            .out_neighbours.name
        )
        expected_names = ["d"]
        assert result_names == expected_names

        filter_expr = filter.Node.property("prop1") < 10
        result_names = sorted(
            graph.node("a").filter(filter_expr).out_neighbours.name
        )
        expected_names = []
        assert result_names == expected_names

    return check


@with_disk_variants(create_test_graph, variants=["graph", "persistent_graph"])
def test_in_neighbours_composite_filter():
    def check(graph):
        filter_expr1 = filter.Node.name() == "a"
        filter_expr2 = filter.Node.property("prop1") > 10
        result_names = sorted(
            graph.node("d").filter(filter_expr1 & filter_expr2).in_neighbours.name
        )
        expected_names = ["a"]
        assert result_names == expected_names

        filter_expr = filter.Node.property("prop1") > 10
        result_names = sorted(
            graph.node("d").filter(filter_expr).in_neighbours.name
        )
        expected_names = ["a", "c"]
        assert result_names == expected_names

        filter_expr = filter.Node.property("prop1") < 10
        result_names = sorted(
            graph.node("d").filter(filter_expr).in_neighbours.name
        )
        expected_names = []
        assert result_names == expected_names

    return check


@with_disk_variants(create_test_graph, variants=["graph", "persistent_graph"])
def test_neighbours_composite_filter():
    def check(graph):
        filter_expr = filter.Node.property("prop4") == False
        result_names = sorted(graph.node("a").filter(filter_expr).neighbours.name)
        expected_names = ["d"]
        assert result_names == expected_names

        filter_expr = filter.Node.property("prop1") <= 100
        result_names = sorted(graph.node("d").filter(filter_expr).neighbours.name)
        expected_names = ["a", "b", "c"]
        assert result_names == expected_names

        filter_expr = filter.Node.property("prop1") > 500
        result_names = sorted(graph.node("d").filter(filter_expr).neighbours.name)
        expected_names = []
        assert result_names == expected_names

    return check
