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

    The comparator sorts iterables, so the list is joined into a string first —
    a reordering would then be a string diff rather than compare equal.
    """
    assert_parity(
        typed_pair,
        lambda g: ",".join(str(v) for v in g.node("a").properties.get("p_list")),
    )


# --- non-finite floats ------------------------------------------------------


def _build_non_finite(g):
    g.add_node(
        1,
        "a",
        properties={
            "nan": float("nan"),
            "pos_inf": float("inf"),
            "neg_inf": float("-inf"),
            "nan_f32": Prop.f32(float("nan")),
            "pos_inf_f32": Prop.f32(float("inf")),
        },
    )


@pytest.fixture(scope="module")
def non_finite_pair():
    with graph_pair(_build_non_finite) as pair:
        yield pair


NON_FINITE_KEYS = ["nan", "pos_inf", "neg_inf", "nan_f32", "pos_inf_f32"]


@pytest.mark.parametrize("key", NON_FINITE_KEYS)
def test_non_finite_float_parity(non_finite_pair, key):
    # The comparator already folds NaN to a sentinel (NaN != NaN would make a
    # direct compare always fail), and ±inf compares by identity.
    assert_parity(non_finite_pair, lambda g: g.node("a").properties.get(key))


@pytest.mark.parametrize("key", ["nan", "nan_f32"])
def test_nan_is_nan_on_both_sides(non_finite_pair, key):
    """Explicitly assert NaN-ness per side — a `None` would silently pass the
    comparator's sentinel folding if both sides degraded the same way."""
    for side in (non_finite_pair.local, non_finite_pair.remote):
        value = side.node("a").properties.get(key)
        assert isinstance(value, float) and math.isnan(
            value
        ), f"expected NaN for {key!r}, got {value!r}"


def test_infinities_are_infinite_on_both_sides(non_finite_pair):
    for side in (non_finite_pair.local, non_finite_pair.remote):
        props = side.node("a").properties
        assert props.get("pos_inf") == math.inf
        assert props.get("neg_inf") == -math.inf


# --- map key order ----------------------------------------------------------


def test_map_property_key_order_parity(typed_pair):
    """A dict-valued property keeps its *insertion* order on both sides.

    Joined into a string because the comparator sorts dict keys — the whole
    point here is that the wire format did not re-key the map alphabetically.
    """
    assert_parity(
        typed_pair, lambda g: ",".join(g.node("a").properties.get("p_map").keys())
    )


def test_map_property_key_order_is_insertion_order(typed_pair):
    """Anchor the order to what was written, so both sides sorting alike fails."""
    for side in (typed_pair.local, typed_pair.remote):
        got = list(side.node("a").properties.get("p_map").keys())
        assert got == list(_MAP), f"map key order changed: {got}"


def test_map_property_values_parity(typed_pair):
    assert_parity(typed_pair, lambda g: g.node("a").properties.get("p_map"))
