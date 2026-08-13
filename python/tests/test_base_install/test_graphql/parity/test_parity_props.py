"""Property-value parity: dtype fidelity, non-finite floats, and map key order.

A property written through ``RemoteGraph`` crosses a GraphQL boundary that the
local ``Graph`` never sees, so this is where lossy round-trips would show up:
a ``u8`` widened to ``i64``, an ``f32`` promoted to ``f64``, a ``NaN`` turned
into ``null``, a map re-keyed into sorted order. Each case seeds the *same*
typed value on both sides and asserts the read-back value **and** its
``properties.get_dtype_of(key)`` agree.
"""

import datetime
import math
from decimal import Decimal

import pytest
from raphtory import Prop

from _parity import assert_parity, graph_pair

# A fixed instant, tz-aware, so datetime parity is not clock-dependent.
_DT = datetime.datetime(2021, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)

# Insertion order is deliberately NOT alphabetical — see the map-order test.
_MAP = {"zeta": 1, "alpha": 2, "mid": 3}

_TYPED_PROPS = {
    "p_u8": Prop.u8(7),
    "p_u16": Prop.u16(300),
    "p_i32": Prop.i32(-70000),
    "p_u64": Prop.u64(2**40),
    "p_f32": Prop.f32(1.5),
    "p_datetime": Prop.aware_datetime(_DT),
    "p_decimal": Prop.decimal(Decimal("3.14")),
    "p_list": Prop.list([1, 2, 3]),
    "p_map": Prop.map(_MAP),
}


def _build_typed(g):
    """Seed one node and one edge carrying every typed property."""
    g.add_node(1, "a", properties=dict(_TYPED_PROPS))
    g.add_node(1, "b")
    g.add_edge(2, "a", "b", properties=dict(_TYPED_PROPS))


@pytest.fixture(scope="module")
def typed_pair():
    with graph_pair(_build_typed) as pair:
        yield pair


# --- dtype fidelity ---------------------------------------------------------

_TYPED_KEYS = list(_TYPED_PROPS)


@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_node_typed_property_value_parity(typed_pair, key):
    assert_parity(typed_pair, lambda g: g.node("a").properties.get(key))


@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_node_typed_property_dtype_parity(typed_pair, key):
    # `repr(PropType)` rather than the object: it renders the full shape
    # (`U8`, `Decimal(2)`, `List<I64>`, `Map{...}`), so a widened or erased
    # type shows up in the diff instead of comparing equal by coincidence.
    # `repr` and not `str`: `str` of a Map dtype iterates a hash map, so its
    # key order varies between calls *on the same graph* — `repr` sorts.
    assert_parity(typed_pair, lambda g: repr(g.node("a").properties.get_dtype_of(key)))


@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_edge_typed_property_dtype_parity(typed_pair, key):
    assert_parity(
        typed_pair, lambda g: repr(g.edge("a", "b").properties.get_dtype_of(key))
    )


@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_typed_property_dtype_object_parity(typed_pair, key):
    """The `PropType` values themselves compare equal, not just their rendering."""
    local = typed_pair.local.node("a").properties.get_dtype_of(key)
    remote = typed_pair.remote.node("a").properties.get_dtype_of(key)
    assert local == remote, f"dtype mismatch for {key!r}: {local!r} != {remote!r}"


def test_absent_property_dtype_parity(typed_pair):
    """A missing key yields `None` on both sides, not an error on one."""
    assert_parity(typed_pair, lambda g: g.node("a").properties.get_dtype_of("nope"))


def test_typed_property_dtypes_are_exact(typed_pair):
    """Pin the *expected* widths, so both sides agreeing on a wrong type fails.

    Parity alone cannot catch a loss that happens identically on both sides;
    this anchors the local (ground-truth) side to the widths that were written.
    """
    expected = {
        "p_u8": "PropType.U8",
        "p_u16": "PropType.U16",
        "p_i32": "PropType.I32",
        "p_u64": "PropType.U64",
        "p_f32": "PropType.F32",
        "p_datetime": "PropType.DTime",
        "p_decimal": "PropType.Decimal(2)",
        "p_list": "PropType.List<I64>",
        "p_map": "PropType.Map{ alpha: I64, mid: I64, zeta: I64 }",
    }
    props = typed_pair.local.node("a").properties
    for key, dtype in expected.items():
        assert repr(props.get_dtype_of(key)) == dtype


def test_list_property_order_parity(typed_pair):
    """List element *order* survives, not just the multiset of elements.

    Compared directly: the comparator only reorders entity collections, so a
    list property keeps its order all the way to the assertion.
    """
    assert_parity(typed_pair, lambda g: g.node("a").properties.get("p_list"))


# --- non-finite floats ------------------------------------------------------


# Every non-finite float, at both widths, and the value it must read back as.
_NON_FINITE = {
    "nan": (float("nan"), math.nan),
    "pos_inf": (float("inf"), math.inf),
    "neg_inf": (float("-inf"), -math.inf),
    "nan_f32": (Prop.f32(float("nan")), math.nan),
    "pos_inf_f32": (Prop.f32(float("inf")), math.inf),
    "neg_inf_f32": (Prop.f32(float("-inf")), -math.inf),
}


def _build_non_finite(g):
    g.add_node(
        1, "a", properties={k: written for k, (written, _) in _NON_FINITE.items()}
    )


@pytest.fixture(scope="module")
def non_finite_pair():
    with graph_pair(_build_non_finite) as pair:
        yield pair


@pytest.mark.parametrize("key", list(_NON_FINITE))
def test_non_finite_float_round_trips(non_finite_pair, key):
    """Each non-finite float reads back as itself, on both sides.

    Asserted per side against an expected value rather than through the
    comparator: ``nan != nan`` in IEEE arithmetic, so a parity comparison would
    fail on two *correct* answers. Pinning the value is also strictly stronger,
    because it catches both sides degrading the same way — a ``None``, a
    ``0.0``, or a ``NaN`` where an infinity was written.
    """
    expected = _NON_FINITE[key][1]
    for name, side in (
        ("local", non_finite_pair.local),
        ("remote", non_finite_pair.remote),
    ):
        got = side.node("a").properties.get(key)
        assert isinstance(got, float), f"{name}: {key} read back as {got!r}"
        if math.isnan(expected):
            assert math.isnan(got), f"{name}: expected NaN for {key}, got {got!r}"
        else:
            assert (
                got == expected
            ), f"{name}: expected {expected} for {key}, got {got!r}"


# --- map key order ----------------------------------------------------------


def test_map_property_key_order_parity(typed_pair):
    """A dict-valued property keeps its *insertion* order on both sides.

    The keys are listed rather than compared as a dict: ``dict.__eq__`` ignores
    order, so comparing the mappings would pass even if the wire format re-keyed
    the map alphabetically. The key *list* is order-sensitive.
    """
    assert_parity(
        typed_pair, lambda g: list(g.node("a").properties.get("p_map").keys())
    )


def test_map_property_key_order_is_insertion_order(typed_pair):
    """Anchor the order to what was written, so both sides sorting alike fails."""
    for side in (typed_pair.local, typed_pair.remote):
        got = list(side.node("a").properties.get("p_map").keys())
        assert got == list(_MAP), f"map key order changed: {got}"


def test_map_property_values_parity(typed_pair):
    assert_parity(typed_pair, lambda g: g.node("a").properties.get("p_map"))
