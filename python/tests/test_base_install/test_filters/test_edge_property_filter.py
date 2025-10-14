from filters_setup import U32_MAX, U64_MAX, I64_MAX, U16_MAX, U8_MAX
from raphtory import filter, Prop
from filters_setup import init_graph, create_test_graph2
from utils import with_disk_variants
import pytest


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_eq():
    def check(graph):
        filter_expr = filter.Edge.property("p2") == 2
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted([("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_ne():
    def check(graph):
        filter_expr = filter.Edge.property("p2") != 2
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_lt():
    def check(graph):
        filter_expr = filter.Edge.property("p2") < 10
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("2", "3"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_le():
    def check(graph):
        filter_expr = filter.Edge.property("p2") <= 6
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("2", "3"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_gt():
    def check(graph):
        filter_expr = filter.Edge.property("p2") > 2
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_edges_for_property_ge():
    def check(graph):
        filter_expr = filter.Edge.property("p2") >= 2
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("2", "3"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_in():
    def check(graph):
        filter_expr = filter.Edge.property("p2").is_in([])
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p2").is_in([0])
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p2").is_in([6])
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("2", "1"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p2").is_in([2, 6])
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("2", "1"),
                ("2", "3"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_not_in():
    def check(graph):
        filter_expr = filter.Edge.property("p2").is_not_in([6])
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_is_some():
    def check(graph):
        filter_expr = filter.Edge.property("p2").is_some()
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("1", "2"),
                ("2", "1"),
                ("2", "3"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_is_none():
    def check(graph):
        filter_expr = filter.Edge.property("p3").is_none()
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = sorted([("1", "2"), ("2", "3")])
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_starts_with():
    def check(graph):
        filter_expr = filter.Edge.property("p10").starts_with("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().any().starts_with("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().last().starts_with("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = (
            filter.Edge.property("p10").temporal().last().starts_with("Rapper")
        )
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p20").temporal().first().starts_with("Gold")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [
            ("1", "2"),
            ("2", "3"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page"),
        ]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p20").temporal().all().starts_with("Gold")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [
            ("1", "2"),
            ("2", "3"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page"),
        ]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.metadata("p10").starts_with("Paper")
        with pytest.raises(
            Exception,
            match=r"Metadata p10 does not exist",
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_ends_with():
    def check(graph):
        filter_expr = filter.Edge.property("p10").ends_with("ship")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().any().ends_with("lane")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().last().ends_with("ane")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().last().ends_with("kane")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = []
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p20").temporal().first().ends_with("boat")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("2", "3"), ("David Gilmour", "John Mayer")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p20").temporal().all().ends_with("ship")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("John Mayer", "Jimmy Page")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.metadata("p10").ends_with("hip")
        with pytest.raises(
            Exception,
            match=r"Metadata p10 does not exist",
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_contains():
    def check(graph):
        filter_expr = filter.Edge.property("p10").contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().any().contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().last().contains("Paper")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1"), ("2", "3")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p20").temporal().first().contains("boat")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("2", "3"), ("David Gilmour", "John Mayer")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.metadata("p10").contains("Paper")
        with pytest.raises(
            Exception,
            match=r"Metadata p10 does not exist",
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_property_not_contains():
    def check(graph):
        filter_expr = filter.Edge.property("p10").not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().any().not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.property("p10").temporal().last().not_contains("ship")
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("2", "1")]
        assert result_ids == expected_ids

        filter_expr = (
            filter.Edge.property("p20").temporal().first().not_contains("boat")
        )
        result_ids = sorted(graph.filter(filter_expr).edges.id)
        expected_ids = [("1", "2"), ("John Mayer", "Jimmy Page")]
        assert result_ids == expected_ids

        filter_expr = filter.Edge.metadata("p10").not_contains("ship")
        with pytest.raises(
            Exception,
            match=r"Metadata p10 does not exist",
        ):
            graph.filter(filter_expr).nodes.id

    return check


@with_disk_variants(init_graph, variants=["graph", "event_disk_graph"])
def test_filter_edges_for_not_property():
    def check(graph):
        filter_expr = filter.Edge.property("p3").is_none()
        result_ids = sorted(graph.filter(~filter_expr).edges.id)
        expected_ids = sorted(
            [
                ("2", "1"),
                ("3", "1"),
                ("3", "4"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result_ids == expected_ids

    return check


def _pairs(edges):
    return {(e.src.name, e.dst.name) for e in edges}


# ------ SUM ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_u8s():
    def check(graph):
        # [1,2,3] -> 6
        expr = filter.Edge.property("p_u8s").sum() == Prop.u64(6)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_u16s():
    def check(graph):
        # [1000,2000] -> 3000
        expr = filter.Edge.property("p_u16s").sum() == Prop.u64(3000)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_u32s():
    def check(graph):
        # [2_000_000,3_000_000] -> 5_000_000
        expr = filter.Edge.property("p_u32s").sum() == Prop.u64(5_000_000)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_u64s():
    def check(graph):
        # [1,2] -> 3
        expr = filter.Edge.property("p_u64s").sum() == Prop.u64(3)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_i32s():
    def check(graph):
        # [1,-2,3] -> 2
        expr = filter.Edge.property("p_i32s").sum() == Prop.i64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_i64s():
    def check(graph):
        # [-1,-2] -> -3
        expr = filter.Edge.property("p_i64s").sum() == Prop.i64(-3)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_f32s():
    def check(graph):
        expr = filter.Edge.property("p_f32s").sum() == Prop.f64(3.0)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_sum_f64s():
    def check(graph):
        # [1.5,2.5] -> 4.0
        expr = filter.Edge.property("p_f64s").sum() == Prop.f64(4.0)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


# ------ AVG ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").avg() == Prop.f64(2.0)  # 6/3
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_u16s():
    def check(graph):
        expr = filter.Edge.property("p_u16s").avg() == Prop.f64(1500.0)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").avg() == Prop.f64(2_500_000.0)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").avg() == Prop.f64(1.5)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").avg() == Prop.f64((1 - 2 + 3) / 3.0)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_i64s():
    def check(graph):
        expr = filter.Edge.property("p_i64s").avg() == Prop.f64((-1 - 2) / 2.0)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_avg_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").avg() == Prop.f64(2.0)  # (1.5+2.5)/2
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


# ------ LEN ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").len() == Prop.u64(3)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_u16s():
    def check(graph):
        expr = filter.Edge.property("p_u16s").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("b", "c"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").len() == Prop.u64(3)
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_i64s():
    def check(graph):
        expr = filter.Edge.property("p_i64s").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_strs():
    def check(graph):
        expr = filter.Edge.property("p_strs").len() == Prop.u64(3)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_bools():
    def check(graph):
        expr = filter.Edge.property("p_bools").len() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_len_zero_on_empty_lists():
    def check(graph):
        expr = filter.Edge.property("p_u8s").len() == Prop.u64(0)
        assert _pairs(graph.filter(expr).edges) == {("c", "d")}

    return check


# ------ MIN ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").min() == Prop.u8(1)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_u16s():
    def check(graph):
        expr = filter.Edge.property("p_u16s").min() == Prop.u16(1000)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").min() == Prop.u32(1_000_000)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").min() == Prop.u64(1)
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_i32s():
    def check(graph):
        # [-100,0,100] on (d->a)
        expr = filter.Edge.property("p_i32s").min() == Prop.i32(-100)
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_i64s():
    def check(graph):
        expr = filter.Edge.property("p_i64s").min() == Prop.i64(-2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_min_f64s():
    def check(graph):
        # [-1.5,0.0,1.5] on (d->a)
        expr = filter.Edge.property("p_f64s").min() == Prop.f64(-1.5)
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


# ------ MAX ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_u8s():
    def check(graph):
        # [255] for (d->a)
        expr = filter.Edge.property("p_u8s").max() == Prop.u8(255)
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_u16s():
    def check(graph):
        expr = filter.Edge.property("p_u16s").max() == Prop.u16(65535)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").max() == Prop.u32(U32_MAX)
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").max() == Prop.u64(U64_MAX)
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").max() == Prop.i32(2_147_483_647)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_i64s():
    def check(graph):
        expr = filter.Edge.property("p_i64s").max() == Prop.i64(I64_MAX)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_max_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").max() == Prop.f64(3.0)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


# ------ last ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_last_sum_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().last().sum() == Prop.u64(30)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("b", "c"), ("c", "d")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_last_avg_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").temporal().last().avg() == Prop.f64(
            0.6666666666666666
        )
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_last_min_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").temporal().last().min() == Prop.u8(1)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_last_max_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").temporal().last().max() == Prop.f64(1.5)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_last_len_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().last().len() == Prop.u64(2)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b"), ("b", "c"), ("d", "a")}

    return check


# ------ all ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_all_sum_i64s():
    def check(graph):
        expr = filter.Edge.property("p_i64s").temporal().all().sum() == Prop.i64(-3)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_all_avg_f32s():
    def check(graph):
        expr = filter.Edge.property("p_f32s").temporal().all().avg() == Prop.f64(2.0)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_all_min_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().all().min() == Prop.u64(1)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_all_max_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").temporal().all().max() == Prop.u32(
            3_000_000
        )
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_all_len_u16s():
    def check(graph):
        expr = filter.Edge.property("p_u16s").temporal().all().len() == Prop.u64(2)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


# ------ first ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_first_sum_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").temporal().first().sum() == Prop.u64(6)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_first_avg_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().first().avg() == Prop.f64(30.0)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("c", "d")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_first_min_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").temporal().first().min() == Prop.i32(-2)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_first_max_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").temporal().first().max() == Prop.f64(1.5)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_first_len_u32s():
    def check(graph):
        expr = filter.Edge.property("p_u32s").temporal().first().len() == Prop.u64(0)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("c", "d")}

    return check


# ------ any ------
@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_any_sum_u64s():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().any().sum() == Prop.u64(3)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_any_avg_i32s():
    def check(graph):
        expr = filter.Edge.property("p_i32s").temporal().any().avg() == Prop.f64(0.0)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_any_min_f32s():
    def check(graph):
        expr = filter.Edge.property("p_f32s").temporal().any().min() == Prop.f32(-1.5)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_any_max_u8s():
    def check(graph):
        expr = filter.Edge.property("p_u8s").temporal().any().max() == Prop.u8(U8_MAX)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_temporal_any_len_f64s():
    def check(graph):
        expr = filter.Edge.property("p_f64s").temporal().any().len() == Prop.u64(3)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_empty_list_agg():
    def check(graph):
        expr = filter.Edge.property("p_i64s").len() == Prop.u64(0)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {("c", "d")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_overflow_max_values():
    def check(graph):
        # max checks
        assert _pairs(
            graph.filter(
                filter.Edge.property("p_u64s").max() == Prop.u64(U64_MAX)
            ).edges
        ) == {("d", "a")}
        assert _pairs(
            graph.filter(
                filter.Edge.property("p_u32s").max() == Prop.u32(U32_MAX)
            ).edges
        ) == {("d", "a")}
        assert _pairs(
            graph.filter(
                filter.Edge.property("p_u16s").max() == Prop.u16(U16_MAX)
            ).edges
        ) == {("b", "c")}
        assert _pairs(
            graph.filter(filter.Edge.property("p_u8s").max() == Prop.u8(U8_MAX)).edges
        ) == {("d", "a")}
        assert _pairs(
            graph.filter(
                filter.Edge.property("p_i64s").max() == Prop.i64(I64_MAX)
            ).edges
        ) == {("b", "c")}

        # overflow SUM for u64: your engine returns None when sum overflows
        pairs = _pairs(
            graph.filter(filter.Edge.property("p_u64s").sum() > Prop.u64(0)).edges
        )
        assert ("d", "a") not in pairs

        # AVG computed in f64 even if sum overflows (mirror node test)
        avg_u64_max_pair = _pairs(
            graph.filter(
                filter.Edge.property("p_u64s").avg() == Prop.f64((U64_MAX + 1.0) / 2.0)
            ).edges
        )
        assert avg_u64_max_pair == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_unsupported_ops_agg():
    def check(graph):
        # STARTS_WITH on SUM
        expr = filter.Edge.property("p_u64s").sum().starts_with("abc")
        with pytest.raises(Exception) as _:
            graph.filter(expr)

        # ENDS_WITH on AVG
        expr = filter.Edge.property("p_u64s").avg().ends_with("abc")
        with pytest.raises(Exception) as _:
            graph.filter(expr)

        # IS_NONE on MIN
        expr = filter.Edge.property("p_u64s").min().is_none()
        with pytest.raises(Exception) as _:
            graph.filter(expr)

        # IS_SOME on MAX
        expr = filter.Edge.property("p_u64s").max().is_some()
        with pytest.raises(Exception) as _:
            graph.filter(expr)

        # CONTAINS on LEN
        expr = filter.Edge.property("p_u64s").len().contains("abc")
        with pytest.raises(Exception) as _:
            graph.filter(expr)

        # NOT_CONTAINS on SUM
        expr = filter.Edge.property("p_u64s").sum().not_contains("abc")
        with pytest.raises(Exception) as _:
            graph.filter(expr)

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_any():
    def check(graph):
        expr = filter.Edge.property("p_u8s").any() == Prop.u8(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_property_all():
    def check(graph):
        expr = filter.Edge.property("p_bools").all() == Prop.bool(True)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_first_any():
    def check(graph):
        expr = filter.Edge.property("p_u64s").temporal().first().any() == Prop.u64(2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_first_all():
    def check(graph):
        expr = filter.Edge.property("p_bools").temporal().first().all() == Prop.bool(
            True
        )
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_last_any():
    def check(graph):
        expr = filter.Edge.property("p_i64s").temporal().last().any() == Prop.i64(-2)
        assert _pairs(graph.filter(expr).edges) == {("a", "b")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_last_all():
    def check(graph):
        expr = filter.Edge.property("p_f32s").temporal().last().all() == Prop.f32(3.0)
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_all_any():
    def check(graph):
        expr = filter.Edge.property("p_bools").temporal().all().any() == Prop.bool(
            False
        )
        assert _pairs(graph.filter(expr).edges) == {("a", "b"), ("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_all_all():
    def check(graph):
        expr = filter.Edge.property("p_bools").temporal().all().all() == Prop.bool(
            False
        )
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_any_any():
    def check(graph):
        expr = filter.Edge.property("p_strs").temporal().any().any() == Prop.str("x")
        assert _pairs(graph.filter(expr).edges) == {("b", "c")}

    return check


@with_disk_variants(create_test_graph2, variants=["graph"])
def test_edge_temporal_property_any_all():
    def check(graph):
        expr = filter.Edge.property("p_strs").temporal().any().all() == Prop.str(
            "longword"
        )
        assert _pairs(graph.filter(expr).edges) == {("d", "a")}

    return check


@with_disk_variants(init_graph)
def test_edges_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Edge.property("p2") > 5
        result_ids = _pairs(graph.edges[filter_expr])
        expected_ids = {
            ("2", "1"),
            ("3", "1"),
            ("3", "4"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page"),
        }
        assert result_ids == expected_ids

        filter_expr2 = filter.Edge.property("p20") == "Gold_ship"
        # TODO: Test chained filters for filters that involve windows and layers (when they are supported)
        result_ids = _pairs(graph.edges[filter_expr][filter_expr2])
        expected_ids = {("John Mayer", "Jimmy Page")}
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = _pairs(graph.edges[filter_expr3])
        expected_ids = {("John Mayer", "Jimmy Page")}
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_exploded_edges_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Edge.property("p2") == 4
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4

        # Test 1
        result_ids = graph.edges[filter_expr].explode()[filter_expr2].id.collect()
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids
        result_ids = graph.edge("1", "2").explode()[filter_expr2].properties["p2"]
        expected_ids = [4]
        assert result_ids == expected_ids

        result_ids = graph.edges[filter_expr].explode()[filter_expr2].id.collect()
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids
        result_ids = graph.edge("1", "2").explode()[filter_expr2].properties["p2"]
        expected_ids = [4]
        assert result_ids == expected_ids

        result_ids = graph.edge("1", "2").explode()[filter_expr2].id.collect()
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids
        result_ids = graph.edge("1", "2").explode()[filter_expr2].properties["p2"]
        expected_ids = [4]
        assert result_ids == expected_ids

        # Test 2
        filter_expr = filter.ExplodedEdge.property("p20") == "Gold_ship"
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4
        result_ids = (
            graph.edge("1", "2").explode()[filter_expr][filter_expr2].id.collect()
        )
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids

        filter_expr = filter.ExplodedEdge.property("p20") == "Gold_ship"
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4
        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.edge("1", "2").explode()[filter_expr3].id.collect()
        expected_ids = [("1", "2")]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_nested_edges_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Edge.property("p2") > 5
        result_ids = graph.nodes.edges[filter_expr].id.collect()
        expected_ids = [
            [("2", "1"), ("3", "1")],
            [("2", "1")],
            [("3", "1"), ("3", "4")],
            [("3", "4")],
            [("David Gilmour", "John Mayer")],
            [("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

        filter_expr2 = filter.Edge.property("p20") == "Gold_ship"
        result_ids = graph.nodes.edges[filter_expr][filter_expr2].id.collect()
        expected_ids = [
            [],
            [],
            [],
            [],
            [],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.nodes.edges[filter_expr3].id.collect()
        expected_ids = [
            [],
            [],
            [],
            [],
            [],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_nested_exploded_edges_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Edge.property("p2") == 4
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4

        # Test 1
        result_ids = graph.nodes.edges[filter_expr].explode()[filter_expr2].id.collect()
        expected_ids = [[("1", "2")], [("1", "2")], [], [], [], [], []]
        assert result_ids == expected_ids

        result_ids = graph.nodes.edges[filter_expr].explode()[filter_expr2].id.collect()
        expected_ids = [[("1", "2")], [("1", "2")], [], [], [], [], []]
        assert result_ids == expected_ids

        # Test 2
        filter_expr = filter.ExplodedEdge.property("p20") == "Gold_ship"
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4
        result_ids = graph.nodes.edges.explode()[filter_expr][filter_expr2].id.collect()
        expected_ids = [[("1", "2")], [("1", "2")], [], [], [], [], []]
        assert result_ids == expected_ids

        filter_expr = filter.ExplodedEdge.property("p20") == "Gold_ship"
        filter_expr2 = filter.ExplodedEdge.property("p2") == 4
        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.nodes.edges.explode()[filter_expr3].id.collect()
        expected_ids = [[("1", "2")], [("1", "2")], [], [], [], [], []]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph)
def test_nodes_nested_edges_getitem_property_filter_expr():
    def check(graph):
        filter_expr = filter.Edge.property("p2") > 5
        result_ids = graph.nodes.neighbours.edges[filter_expr].id.collect()
        expected_ids = [
            [("2", "1"), ("3", "1"), ("3", "4")],
            [("2", "1"), ("3", "1"), ("3", "1"), ("3", "4")],
            [("2", "1"), ("3", "1"), ("2", "1"), ("3", "4")],
            [("3", "1"), ("3", "4")],
            [("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")],
            [("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")],
            [("David Gilmour", "John Mayer"), ("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

        filter_expr2 = filter.Edge.property("p20") == "Gold_ship"
        result_ids = graph.nodes.neighbours.edges[filter_expr][
            filter_expr2
        ].id.collect()
        expected_ids = [
            [],
            [],
            [],
            [],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

        filter_expr3 = filter_expr & filter_expr2
        result_ids = graph.nodes.neighbours.edges[filter_expr3].id.collect()
        expected_ids = [
            [],
            [],
            [],
            [],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
            [("John Mayer", "Jimmy Page")],
        ]
        assert result_ids == expected_ids

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_edge_property_temporal_sum():
    def check(graph):
        expr = filter.Edge.property("p2").temporal().sum() < 10
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {
            ("2", "3"),
            ("3", "1"),
            ("John Mayer", "Jimmy Page"),
            ("David Gilmour", "John Mayer"),
            ("3", "4"),
            ("1", "2"),
            ("2", "1"),
        }

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_edge_property_temporal_avg():
    def check(graph):
        expr = filter.Edge.property("p2").temporal().avg() == 6.0
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {
            ("3", "4"),
            ("3", "1"),
            ("2", "1"),
            ("John Mayer", "Jimmy Page"),
            ("David Gilmour", "John Mayer"),
        }

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_edge_property_temporal_min():
    def check(graph):
        expr = filter.Edge.property("p3").temporal().min() == 1
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {
            ("3", "4"),
            ("John Mayer", "Jimmy Page"),
            ("2", "1"),
            ("3", "1"),
            ("David Gilmour", "John Mayer"),
        }

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_edge_property_temporal_max():
    def check(graph):
        expr = filter.Edge.property("p2").temporal().max() == 6
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {
            ("3", "4"),
            ("2", "1"),
            ("3", "1"),
            ("David Gilmour", "John Mayer"),
            ("John Mayer", "Jimmy Page"),
        }

    return check


@with_disk_variants(init_graph, variants=["graph"])
def test_edge_property_temporal_len():
    def check(graph):
        expr = filter.Edge.property("p3").temporal().len() == Prop.u64(1)
        pairs = _pairs(graph.filter(expr).edges)
        assert pairs == {
            ("3", "4"),
            ("John Mayer", "Jimmy Page"),
            ("David Gilmour", "John Mayer"),
            ("3", "1"),
            ("2", "1"),
        }

    return check
