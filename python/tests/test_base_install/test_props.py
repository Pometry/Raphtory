from raphtory import Prop
from utils import expect_unify_error, assert_in_all
from decimal import Decimal
import pytest


def test_list_u64s():
    xs = Prop.list([Prop.u64(1), Prop.u64(2), Prop.u64(3)])
    assert xs.dtype() == "List(U64)"
    assert repr(xs) == "[1, 2, 3]"


def test_list_i32s():
    xs = Prop.list([Prop.i32(-12), Prop.i32(0), Prop.i32(7)])
    assert xs.dtype() == "List(I32)"
    r = repr(xs)
    assert repr(xs) == "[-12, 0, 7]"


def test_list_from_python_ints():
    xs = Prop.list([1, 2, 3, 4])
    assert xs.dtype() == "List(I64)"
    assert repr(xs) == "[1, 2, 3, 4]"


def test_list_from_python_f64s():
    xs = Prop.list([1.5, 2.5, 3.0])
    assert xs.dtype() == "List(F64)"
    assert repr(xs) == "[1.5, 2.5, 3]"


def test_list_from_strings():
    xs = Prop.list(["a", "bb", "ccc"])
    assert xs.dtype() == "List(Str)"
    assert repr(xs) == '["a", "bb", "ccc"]'


def test_list_from_bools():
    xs = Prop.list([True, False, True])
    assert xs.dtype() == "List(Bool)"
    assert repr(xs) == "[true, false, true]"


def test_list_from_decimals():
    xs = Prop.list([Decimal("1.25"), Decimal("2.50")])
    assert xs.dtype() == "List(Decimal { scale: 2 })"
    assert repr(xs) == "[Decimal(2), Decimal(2)]"


def test_list_empty():
    xs = Prop.list([])
    assert xs.dtype() == "List(Empty)"
    assert repr(xs) == "[]"


def test_list_rejects_heterogeneous_mixed_variants():
    expect_unify_error(lambda: Prop.list([Prop.u8(7), Prop.u16(65535)]).dtype())


def test_list_rejects_heterogeneous_python_mixed_scalars():
    expect_unify_error(lambda: Prop.list([1, "Shivam"]).dtype())


def test_map_u64s():
    xs = Prop.map({"a": Prop.u64(1), "b": Prop.u64(2), "c": Prop.u64(3)})
    assert_in_all(xs.dtype(), ['"a": U64', '"b": U64', '"c": U64'])
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3'])


def test_map_i32s():
    xs = Prop.map({"neg": Prop.i32(-12), "zero": Prop.i32(0), "pos": Prop.i32(7)})
    assert_in_all(xs.dtype(), ['"neg": I32', '"zero": I32', '"pos": I32'])
    assert_in_all(repr(xs), ['"neg": -12', '"zero": 0', '"pos": 7'])


def test_map_from_python_ints():
    xs = Prop.map({"a": 1, "b": 2, "c": 3, "d": 4})
    assert_in_all(xs.dtype(), ['"a": I64', '"b": I64', '"c": I64', '"d": I64'])
    assert_in_all(repr(xs), ['"a": 1', '"b": 2', '"c": 3', '"d": 4'])


def test_map_from_python_f64s():
    xs = Prop.map({"x": 1.5, "y": 2.5, "z": 3.0})
    assert_in_all(xs.dtype(), ['"x": F64', '"y": F64', '"z": F64'])
    assert_in_all(repr(xs), ['"x": 1.5', '"y": 2.5', '"z": 3'])


def test_map_from_strings():
    xs = Prop.map({"k1": "a", "k2": "bb", "k3": "ccc"})
    assert_in_all(xs.dtype(), ['"k1": Str', '"k2": Str', '"k3": Str'])
    assert_in_all(repr(xs), ['"k1": "a"', '"k2": "bb"', '"k3": "ccc"'])


def test_map_from_bools():
    xs = Prop.map({"t": True, "f": False, "t2": True})
    assert_in_all(xs.dtype(), ['"t": Bool', '"f": Bool', '"t2": Bool'])
    assert_in_all(repr(xs), ['"t": true', '"f": false', '"t2": true'])


def test_map_from_decimals():
    xs = Prop.map({"a": Decimal("1.25"), "b": Decimal("2.50")})
    assert_in_all(
        xs.dtype(), ['"a": Decimal { scale: 2 }', '"b": Decimal { scale: 2 }']
    )
    assert_in_all(repr(xs), ['"a": Decimal(2)', '"b": Decimal(2)'])


def test_map_empty():
    xs = Prop.map({})
    assert xs.dtype() == "Map({})"
    assert repr(xs) == "{}"


def test_map_rejects_non_string_keys():
    with pytest.raises(TypeError):
        Prop.map({1: Prop.u64(1)})


def test_map_allows_heterogeneous_mixed_variants():
    xs = Prop.map({"a": Prop.u8(7), "b": Prop.u16(65535)})
    assert_in_all(xs.dtype(), ['"a": U8', '"b": U16'])
    assert_in_all(repr(xs), ['"a": 7', '"b": 65535'])


def test_map_allows_heterogeneous_python_mixed_scalars():
    xs = Prop.map({"a": 1, "b": "Shivam"})
    assert_in_all(xs.dtype(), ['"a": I64', '"b": Str'])
    assert_in_all(repr(xs), ['"a": 1', '"b": "Shivam"'])


def test_map_with_nested_lists_homogeneous_inner_ok():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.u64(3)]),
        }
    )
    assert_in_all(xs.dtype(), ['"x": List(U64)', '"y": List(U64)'])
    assert_in_all(repr(xs), ['"x": [1, 2]', '"y": [3]'])


def test_map_with_nested_lists_values_can_have_different_inner_types():
    xs = Prop.map(
        {
            "x": Prop.list([Prop.u64(1), Prop.u64(2)]),
            "y": Prop.list([Prop.i64(-1), Prop.i64(0)]),
        }
    )
    assert_in_all(xs.dtype(), ['"x": List(U64)', '"y": List(I64)'])
    assert_in_all(repr(xs), ['"x": [1, 2]', '"y": [-1, 0]'])


def test_map_with_nested_list_that_is_heterogeneous_rejected():
    expect_unify_error(
        lambda: Prop.map(
            {
                "bad": Prop.list([Prop.u64(1), Prop.i64(-1)]),
            }
        ).dtype()
    )
