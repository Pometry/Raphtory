from raphtory import filter
from filters_setup import init_graph
from utils import with_disk_variants


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edge_composite_filter():
    def check(graph):
        filter_expr1 = filter.Edge.property("p2") == 2
        filter_expr2 = filter.Edge.property("p1") == "kapoor"
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr1 = filter.Edge.property("p2") > 2
        filter_expr2 = filter.Edge.property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter(filter_expr1 | filter_expr2).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("3", "1"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

        # TODO: Enable this test once string property is fixed for disk_storage_graph
        #     filter_expr1 = filter.Edge.property("p2") < 9
        #     filter_expr2 = filter.Edge.property("p1") == "shivam_kapoor"
        #     result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).edges.id)
        #     expected_ids = [("1", "2")]
        #     assert result_ids == expected_ids

        filter_expr1 = filter.Edge.property("p2") < 9
        filter_expr2 = filter.Edge.property("p3") < 9
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).edges.id)
        expected_ids = [
            ("2", "1"),
            ("3", "1"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page"),
        ]
        assert result_ids == expected_ids

        # TODO: Enable this test once string property is fixed for disk_storage_graph
        #     filter_expr1 = filter.Edge.src().name() == "1"
        #     filter_expr2 = filter.Edge.property("p1") == "shivam_kapoor"
        #     result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).edges.id)
        #     expected_ids = [("1", "2")]
        #     assert result_ids == expected_ids

        filter_expr1 = filter.Edge.dst().name() == "1"
        filter_expr2 = filter.Edge.property("p2") <= 6
        result_ids = sorted(graph.filter(filter_expr1 & filter_expr2).edges.id)
        expected_ids = sorted([("2", "1"), ("3", "1")])
        assert result_ids == expected_ids

    # TODO: Enable this test once string property is fixed for disk_storage_graph
    #     filter_expr1 = filter.Edge.src().name() == "1"
    #     filter_expr2 = filter.Edge.property("p1") == "shivam_kapoor"
    #     filter_expr3 = filter.Edge.property("p3") == 5
    #     result_ids = sorted(graph.filter((filter_expr1 & filter_expr2) | filter_expr3).edges.id)
    #     expected_ids = [("1", "2")]
    #     assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_not_edge_composite_filter():
    def check(graph):
        filter_expr1 = filter.Edge.dst().name() == "1"
        filter_expr2 = filter.Edge.property("p2") <= 2
        result_ids = sorted(graph.filter(~filter_expr1 & filter_expr2).edges.id)
        expected_ids = [("2", "3")]
        assert result_ids == expected_ids

        filter_expr1 = filter.Edge.dst().name() == "1"
        filter_expr2 = filter.Edge.property("p2") <= 6
        result_ids = sorted(graph.filter(~(filter_expr1 & filter_expr2)).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "3"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check
