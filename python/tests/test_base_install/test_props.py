from raphtory import Prop
from utils import expect_unify_error


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
    from decimal import Decimal

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
