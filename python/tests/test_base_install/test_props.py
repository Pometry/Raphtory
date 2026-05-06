from raphtory import Graph, Prop
from utils import expect_unify_error, assert_in_all
from decimal import Decimal
from datetime import datetime, timezone
import pytest
import numpy as np


def test_list_u64s():
    xs = Prop.list([Prop.u64(1), Prop.u64(2), Prop.u64(3)])
    assert str(xs.dtype()) == "List(U64)"
    assert repr(xs) == "[1, 2, 3]"


def test_list_i32s():
    xs = Prop.list([Prop.i32(-12), Prop.i32(0), Prop.i32(7)])
    assert str(xs.dtype()) == "List(I32)"
    r = repr(xs)
    assert repr(xs) == "[-12, 0, 7]"


def test_list_from_python_ints():
    xs = Prop.list([1, 2, 3, 4])
    assert str(xs.dtype()) == "List(I64)"
    assert repr(xs) == "[1, 2, 3, 4]"


def test_list_from_python_f64s():
    xs = Prop.list([1.5, 2.5, 3.0])
    assert str(xs.dtype()) == "List(F64)"
    assert repr(xs) == "[1.5, 2.5, 3]"


def test_list_from_strings():
    xs = Prop.list(["a", "bb", "ccc"])
    assert str(xs.dtype()) == "List(Str)"
    assert repr(xs) == '["a", "bb", "ccc"]'


def test_list_from_bools():
    xs = Prop.list([True, False, True])
    assert str(xs.dtype()) == "List(Bool)"
    assert repr(xs) == "[true, false, true]"


def test_list_from_decimals():
    xs = Prop.list([Decimal("1.25"), Decimal("2.50")])
    assert str(xs.dtype()) == "List(Decimal { scale: 2 })"
    assert repr(xs) == "[Decimal(2), Decimal(2)]"


def test_list_empty():
    xs = Prop.list([])
    assert str(xs.dtype()) == "List(Empty)"
    assert repr(xs) == "[]"


def test_list_rejects_heterogeneous_mixed_variants():
    expect_unify_error(lambda: Prop.list([Prop.u8(7), Prop.u16(65535)]).dtype())


def test_list_rejects_heterogeneous_python_mixed_scalars():
    expect_unify_error(lambda: Prop.list([1, "Shivam"]).dtype())


def test_map_u64s():
    xs = Prop.map({"a": Prop.u64(1), "b": Prop.u64(2), "c": Prop.u64(3)})
    assert_in_all(str(xs.dtype()), ['"a": U64', '"b": U64', '"c": U64'])
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3'])


def test_map_i32s():
    xs = Prop.map({"neg": Prop.i32(-12), "zero": Prop.i32(0), "pos": Prop.i32(7)})
    assert_in_all(str(xs.dtype()), ['"neg": I32', '"zero": I32', '"pos": I32'])
    assert_in_all(repr(xs), ['"neg": -12', '"zero": 0', '"pos": 7'])


def test_map_from_python_ints():
    xs = Prop.map({"a": 1, "b": 2, "c": 3, "d": 4})
    assert_in_all(str(xs.dtype()), ['"a": I64', '"b": I64', '"c": I64', '"d": I64'])
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3', '"d": 4'])


def test_map_from_python_f64s():
    xs = Prop.map({"x": 1.5, "y": 2.5, "z": 3.0})
    assert_in_all(str(xs.dtype()), ['"x": F64', '"y": F64', '"z": F64'])
    assert_in_all(repr(xs), ['"x": 1.5', '"y": 2.5', '"z": 3'])


def test_map_from_strings():
    xs = Prop.map({"k1": "a", "k2": "bb", "k3": "ccc"})
    assert_in_all(str(xs.dtype()), ['"k1": Str', '"k2": Str', '"k3": Str'])
    assert_in_all(repr(xs), ['"k1": "a"', '"k2": "bb"', '"k3": "ccc"'])


def test_map_from_bools():
    xs = Prop.map({"t": True, "f": False, "t2": True})
    assert_in_all(str(xs.dtype()), ['"t": Bool', '"f": Bool', '"t2": Bool'])
    assert_in_all(repr(xs), ['"t": true', '"f": false', '"t2": true'])


def test_map_from_decimals():
    xs = Prop.map({"a": Decimal("1.25"), "b": Decimal("2.50")})
    assert_in_all(
        str(xs.dtype()), ['"a": Decimal { scale: 2 }', '"b": Decimal { scale: 2 }']
    )
    assert_in_all(repr(xs), ['"a": Decimal(2)', '"b": Decimal(2)'])


def test_map_empty():
    xs = Prop.map({})
    assert str(xs.dtype()) == "Map({})"
    assert repr(xs) == "{}"


def test_map_rejects_non_string_keys():
    with pytest.raises(TypeError):
        Prop.map({1: Prop.u64(1)})


def test_map_allows_heterogeneous_mixed_variants():
    xs = Prop.map({"a": Prop.u8(7), "b": Prop.u16(65535)})
    assert_in_all(str(xs.dtype()), ['"a": U8', '"b": U16'])
    assert_in_all(repr(xs), ['"a": 7', '"b": 65535'])


def test_map_allows_heterogeneous_python_mixed_scalars():
    xs = Prop.map({"a": 1, "b": "Shivam"})
    assert_in_all(str(xs.dtype()), ['"a": I64', '"b": Str'])
    assert_in_all(repr(xs), ['"a": 1', '"b": "Shivam"'])


def test_map_with_nested_lists_homogeneous_inner_ok():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.u64(3)]),
        }
    )
    assert_in_all(str(xs.dtype()), ['"x": List(U64)', '"y": List(U64)'])
    assert_in_all(repr(xs), ['"x": [1, 2]', '"y": [3]'])


def test_map_with_nested_lists_values_can_have_different_inner_types():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.i64(-1), Prop.i64(0)]),
        }
    )
    assert_in_all(str(xs.dtype()), ['"x": List(U64)', '"y": List(I64)'])
    assert_in_all(repr(xs), ['"x": [1, 2]', '"y": [-1, 0]'])


def test_map_with_nested_list_that_is_heterogeneous_rejected():
    expect_unify_error(
        lambda: Prop.map(
            {
                "bad": Prop.list([Prop.u64(1), Prop.i64(-1)]),
            }
        ).dtype()
    )


def test_aware_datetime():
    dt = datetime(2024, 6, 1, 12, 30, 45, tzinfo=timezone.utc)
    p = Prop.aware_datetime(dt)
    assert str(p.dtype()) == "DTime"
    assert "2024-06-01" in repr(p)


def test_aware_datetime_treats_naive_as_utc():
    """Naive datetimes are accepted and interpreted as UTC, consistent with
    how `EventTime` and other Raphtory time inputs handle them."""
    naive = datetime(2024, 6, 1, 12, 30, 45)
    aware = datetime(2024, 6, 1, 12, 30, 45, tzinfo=timezone.utc)
    assert Prop.aware_datetime(naive) == Prop.aware_datetime(aware)


def test_naive_datetime():
    dt = datetime(2024, 6, 1, 12, 30, 45)
    p = Prop.naive_datetime(dt)
    assert str(p.dtype()) == "NDTime"
    assert "2024-06-01" in repr(p)


def test_decimal_from_string():
    p = Prop.decimal("1234.5678")
    # Decimal stores scale; dtype reports it.
    assert str(p.dtype()) == "Decimal { scale: 4 }"


def test_decimal_from_negative_string():
    p = Prop.decimal("-0.001")
    assert str(p.dtype()) == "Decimal { scale: 3 }"


def test_decimal_from_string_zero_scale():
    p = Prop.decimal("42")
    assert str(p.dtype()) == "Decimal { scale: 0 }"


def test_decimal_from_python_decimal():
    p = Prop.decimal(Decimal("99.99"))
    assert str(p.dtype()) == "Decimal { scale: 2 }"


def test_decimal_from_python_decimal_high_precision():
    """`decimal.Decimal` preserves precision regardless of float limits."""
    p = Prop.decimal(Decimal("1.234567890123456789012345"))
    assert str(p.dtype()) == "Decimal { scale: 24 }"


def test_decimal_from_int():
    p = Prop.decimal(7)
    assert str(p.dtype()) == "Decimal { scale: 0 }"


def test_decimal_from_negative_int():
    p = Prop.decimal(-42)
    assert str(p.dtype()) == "Decimal { scale: 0 }"


def test_decimal_from_large_int():
    p = Prop.decimal(2 ** 62)
    assert str(p.dtype()) == "Decimal { scale: 0 }"


def test_decimal_from_float():
    p = Prop.decimal(1.5)
    assert "Decimal" in str(p.dtype())


def test_decimal_from_negative_float():
    p = Prop.decimal(-3.25)
    assert "Decimal" in str(p.dtype())


def test_decimal_rejects_non_numeric_string():
    with pytest.raises(TypeError):
        Prop.decimal("not a number")


def test_decimal_rejects_unsupported_type():
    with pytest.raises(TypeError):
        Prop.decimal([1, 2, 3])


def test_decimal_in_graph_roundtrips():
    """Decimal Props attach to graph entities and are readable back."""
    g = Graph()
    g.add_node(1, "n", properties={"price": Prop.decimal("19.99")})
    val = g.node("n").properties.get("price")
    assert val == Decimal("19.99")


def test_decimal_in_graph_from_int_then_read_back():
    g = Graph()
    g.add_node(1, "n", properties={"count": Prop.decimal(42)})
    val = g.node("n").properties.get("count")
    assert val == Decimal("42")


def test_decimal_in_graph_from_float_then_read_back():
    g = Graph()
    g.add_node(1, "n", properties={"ratio": Prop.decimal(1.5)})
    val = g.node("n").properties.get("ratio")
    assert val == Decimal("1.5")


def test_decimal_list_in_graph():
    """Lists of Decimal Props inherit a unified scale."""
    g = Graph()
    g.add_node(
        1,
        "n",
        properties={"prices": Prop.list([Prop.decimal("1.25"), Prop.decimal("2.50")])},
    )
    vals = g.node("n").properties.get("prices")
    assert np.array_equal(vals, [Decimal("1.25"), Decimal("2.50")])


def test_decimal_list_rejects_mixed_scales():
    """Mixing decimal scales in a list errors at unification time."""
    expect_unify_error(
        lambda: Prop.list([Prop.decimal("1.25"), Prop.decimal("2.5")]).dtype()
    )
