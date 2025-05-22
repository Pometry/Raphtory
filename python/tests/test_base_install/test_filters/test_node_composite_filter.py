from raphtory import Prop, filter
import pytest
from filters_setup import init_graph
from utils import with_disk_variants


@with_disk_variants(init_graph)
def test_node_composite_filter():
    def check(graph):
        filter_expr1 = filter.Property("p2") == 2
        filter_expr2 = filter.Property("p1") == "kapoor"
        result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr1 = filter.Property("p2") > 2
        filter_expr2 = filter.Property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter_nodes(filter_expr1 | filter_expr2).nodes.id)
        expected_ids = ["1", "3"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Property("p9") < 9
        filter_expr2 = filter.Property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.node_type() == "fire_nation"
        filter_expr2 = filter.Property("p1") == "shivam_kapoor"
        result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["1"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Property("p2") >= 2
        result_ids = sorted(graph.filter_nodes(filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["2"]
        assert result_ids == expected_ids

        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Property("p2") == 2
        filter_expr3 = filter.Property("p9") <= 5
        result_ids = sorted(graph.filter_nodes((filter_expr1 & filter_expr2) | filter_expr3).nodes.id)
        expected_ids = ["1", "2"]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_not_node_composite_filter():
    def check(graph):
        filter_expr1 = filter.Node.name() == "2"
        filter_expr2 = filter.Property("p2") >= 2
        result_ids = sorted(graph.filter_nodes(~filter_expr1 & filter_expr2).nodes.id)
        expected_ids = ["3"]
        assert result_ids == expected_ids

        result_ids = sorted(graph.filter_nodes(~(filter_expr1 & filter_expr2)).nodes.id)
        expected_ids = sorted(["1", "3", "4", "David Gilmour",  "Jimmy Page", "John Mayer"])
        assert result_ids == expected_ids

    return check

