from raphtory import Graph, Prop, PropType
from utils import expect_unify_error, assert_in_all
from decimal import Decimal
from datetime import datetime, timezone
import pytest
import numpy as np


def test_list_u64s():
    xs = Prop.list([Prop.u64(1), Prop.u64(2), Prop.u64(3)])
    assert xs.dtype() == PropType.list(PropType.u64())
    assert repr(xs) == "[1, 2, 3]"


def test_list_i32s():
    xs = Prop.list([Prop.i32(-12), Prop.i32(0), Prop.i32(7)])
    assert xs.dtype() == PropType.list(PropType.i32())
    r = repr(xs)
    assert repr(xs) == "[-12, 0, 7]"


def test_list_from_python_ints():
    xs = Prop.list([1, 2, 3, 4])
    assert xs.dtype() == PropType.list(PropType.i64())
    assert repr(xs) == "[1, 2, 3, 4]"


def test_list_from_python_f64s():
    xs = Prop.list([1.5, 2.5, 3.0])
    assert xs.dtype() == PropType.list(PropType.f64())
    assert repr(xs) == "[1.5, 2.5, 3]"


def test_list_from_strings():
    xs = Prop.list(["a", "bb", "ccc"])
    assert xs.dtype() == PropType.list(PropType.str())
    assert repr(xs) == '["a", "bb", "ccc"]'


def test_list_from_bools():
    xs = Prop.list([True, False, True])
    assert xs.dtype() == PropType.list(PropType.bool())
    assert repr(xs) == "[true, false, true]"


def test_list_from_decimals():
    xs = Prop.list([Decimal("1.25"), Decimal("2.50")])
    assert xs.dtype() == PropType.list(PropType.decimal(2))
    assert repr(xs) == "[Decimal(2), Decimal(2)]"


def test_list_empty():
    xs = Prop.list([])
    # No `PropType` constructor spells Empty, so this one stays a string check.
    assert str(xs.dtype()) == "List(Empty)"
    assert repr(xs) == "[]"


def test_list_rejects_heterogeneous_mixed_variants():
    expect_unify_error(lambda: Prop.list([Prop.u8(7), Prop.u16(65535)]).dtype())


def test_list_rejects_heterogeneous_python_mixed_scalars():
    expect_unify_error(lambda: Prop.list([1, "Shivam"]).dtype())


def test_map_u64s():
    xs = Prop.map({"a": Prop.u64(1), "b": Prop.u64(2), "c": Prop.u64(3)})
    assert xs.dtype() == PropType.map(
        {"a": PropType.u64(), "b": PropType.u64(), "c": PropType.u64()}
    )
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3'])


def test_map_i32s():
    xs = Prop.map({"neg": Prop.i32(-12), "zero": Prop.i32(0), "pos": Prop.i32(7)})
    assert xs.dtype() == PropType.map(
        {"neg": PropType.i32(), "zero": PropType.i32(), "pos": PropType.i32()}
    )
    assert_in_all(repr(xs), ['"neg": -12', '"zero": 0', '"pos": 7'])


def test_map_from_python_ints():
    xs = Prop.map({"a": 1, "b": 2, "c": 3, "d": 4})
    assert xs.dtype() == PropType.map({k: PropType.i64() for k in ("a", "b", "c", "d")})
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3', '"d": 4'])


def test_map_from_python_f64s():
    xs = Prop.map({"x": 1.5, "y": 2.5, "z": 3.0})
    assert xs.dtype() == PropType.map({k: PropType.f64() for k in ("x", "y", "z")})
    assert_in_all(repr(xs), ['"x": 1.5', '"y": 2.5', '"z": 3'])


def test_map_from_strings():
    xs = Prop.map({"k1": "a", "k2": "bb", "k3": "ccc"})
    assert xs.dtype() == PropType.map({k: PropType.str() for k in ("k1", "k2", "k3")})
    assert_in_all(repr(xs), ['"k1": "a"', '"k2": "bb"', '"k3": "ccc"'])


def test_map_from_bools():
    xs = Prop.map({"t": True, "f": False, "t2": True})
    assert xs.dtype() == PropType.map({k: PropType.bool() for k in ("t", "f", "t2")})
    assert_in_all(repr(xs), ['"t": true', '"f": false', '"t2": true'])


def test_map_from_decimals():
    xs = Prop.map({"a": Decimal("1.25"), "b": Decimal("2.50")})
    assert xs.dtype() == PropType.map(
        {"a": PropType.decimal(2), "b": PropType.decimal(2)}
    )
    assert_in_all(repr(xs), ['"a": Decimal(2)', '"b": Decimal(2)'])


def test_map_empty():
    xs = Prop.map({})
    assert xs.dtype() == PropType.map({})
    assert repr(xs) == "{}"


def test_map_rejects_non_string_keys():
    with pytest.raises(TypeError):
        Prop.map({1: Prop.u64(1)})


def test_map_allows_heterogeneous_mixed_variants():
    xs = Prop.map({"a": Prop.u8(7), "b": Prop.u16(65535)})
    assert xs.dtype() == PropType.map({"a": PropType.u8(), "b": PropType.u16()})
    assert_in_all(repr(xs), ['"a": 7', '"b": 65535'])


def test_map_allows_heterogeneous_python_mixed_scalars():
    xs = Prop.map({"a": 1, "b": "Shivam"})
    assert xs.dtype() == PropType.map({"a": PropType.i64(), "b": PropType.str()})
    assert_in_all(repr(xs), ['"a": 1', '"b": "Shivam"'])


def test_map_with_nested_lists_homogeneous_inner_ok():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.u64(3)]),
        }
    )
    assert xs.dtype() == PropType.map(
        {k: PropType.list(PropType.u64()) for k in ("x", "y")}
    )
    assert_in_all(repr(xs), ['"x": [1, 2]', '"y": [3]'])


def test_map_with_nested_lists_values_can_have_different_inner_types():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.i64(-1), Prop.i64(0)]),
        }
    )
    assert xs.dtype() == PropType.map(
        {"x": PropType.list(PropType.u64()), "y": PropType.list(PropType.i64())}
    )
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
    assert p.dtype() == PropType.datetime()
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
    assert p.dtype() == PropType.naive_datetime()
    assert "2024-06-01" in repr(p)


def test_decimal_from_string():
    p = Prop.decimal("1234.5678")
    # Decimal stores scale; dtype reports it.
    assert p.dtype() == PropType.decimal(4)


def test_decimal_from_negative_string():
    p = Prop.decimal("-0.001")
    assert p.dtype() == PropType.decimal(3)


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
    p = Prop.decimal(2**62)
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
