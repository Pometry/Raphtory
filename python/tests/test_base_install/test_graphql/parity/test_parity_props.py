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

# Fixed instants so datetime parity is not clock-dependent — one tz-aware, one
# naive, because they are distinct dtypes (`DTime` vs `NDTime`) and the naive
# one is the easier of the two to lose across a wire format that assumes UTC.
_DT = datetime.datetime(2021, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)
_NDT = datetime.datetime(2021, 3, 4, 5, 6, 7)

# Insertion order is deliberately NOT alphabetical — see the map-order test.
_MAP = {"zeta": 1, "alpha": 2, "mid": 3}

# Every `Prop` constructor raphtory exposes, so no width or kind goes untested:
# a type absent from this table is one whose round-trip nothing here pins.
_TYPED_PROPS = {
    "p_u8": Prop.u8(7),
    "p_u16": Prop.u16(300),
    "p_u32": Prop.u32(70000),
    "p_u64": Prop.u64(2**40),
    "p_i32": Prop.i32(-70000),
    "p_i64": Prop.i64(-(2**40)),
    "p_f32": Prop.f32(1.5),
    "p_f64": Prop.f64(2.5),
    "p_bool": Prop.bool(True),
    "p_str": Prop.str("hello"),
    "p_datetime": Prop.aware_datetime(_DT),
    "p_naive_datetime": Prop.naive_datetime(_NDT),
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


# `PropType` values compare structurally, so the dtype tests compare the
# objects. (An earlier version compared `repr` strings; that was a workaround
# for `str(PropType)` of a Map being non-deterministic — it iterates a hash map,
# so its key order varies between calls on the same graph. Equality is not
# affected by that, and reports a real difference rather than a rendering one.)
@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_node_typed_property_dtype_parity(typed_pair, key):
    assert_parity(typed_pair, lambda g: g.node("a").properties.get_dtype_of(key))


@pytest.mark.parametrize("key", _TYPED_KEYS)
def test_edge_typed_property_dtype_parity(typed_pair, key):
    assert_parity(typed_pair, lambda g: g.edge("a", "b").properties.get_dtype_of(key))


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
        "p_u32": "PropType.U32",
        "p_u64": "PropType.U64",
        "p_i32": "PropType.I32",
        "p_i64": "PropType.I64",
        "p_f32": "PropType.F32",
        "p_f64": "PropType.F64",
        "p_bool": "PropType.Bool",
        "p_str": "PropType.Str",
        "p_datetime": "PropType.DTime",
        "p_naive_datetime": "PropType.NDTime",
        "p_decimal": "PropType.Decimal(2)",
        "p_list": "PropType.List<I64>",
        "p_map": "PropType.Map{ alpha: I64, mid: I64, zeta: I64 }",
    }
    # `repr` here (not equality): this pins the *expected* widths, and a literal
    # is the readable way to write them down. Unlike `str`, `repr` sorts map
    # keys, so it is stable enough to compare against.
    props = typed_pair.local.node("a").properties
    assert set(expected) == set(_TYPED_PROPS), "every typed property needs a pin"
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


_WRITTEN = {k: written for k, (written, _) in _NON_FINITE.items()}


def _build_non_finite(g):
    """Seed the non-finite floats on every carrier: node, edge and graph, as
    both temporal properties and metadata."""
    g.add_node(1, "a", properties=dict(_WRITTEN))
    g.add_node(1, "b")
    g.add_edge(2, "a", "b", properties=dict(_WRITTEN))
    g.add_properties(3, dict(_WRITTEN))
    g.node("a").add_metadata(dict(_WRITTEN))
    g.edge("a", "b").add_metadata(dict(_WRITTEN))
    g.add_metadata(dict(_WRITTEN))


@pytest.fixture(scope="module")
def non_finite_pair():
    with graph_pair(_build_non_finite) as pair:
        yield pair


# Every place a property can live: (carrier, container) -> reader.
_CARRIERS = {
    "node.properties": lambda g: g.node("a").properties,
    "node.metadata": lambda g: g.node("a").metadata,
    "edge.properties": lambda g: g.edge("a", "b").properties,
    "edge.metadata": lambda g: g.edge("a", "b").metadata,
    "graph.properties": lambda g: g.properties,
    "graph.metadata": lambda g: g.metadata,
}


@pytest.mark.parametrize("carrier", list(_CARRIERS))
@pytest.mark.parametrize("key", list(_NON_FINITE))
def test_non_finite_float_round_trips(non_finite_pair, carrier, key):
    """Each non-finite float reads back as itself, on both sides, everywhere.

    Asserted per side against an expected value rather than through the
    comparator: ``nan != nan`` in IEEE arithmetic, so a parity comparison would
    fail on two *correct* answers. Pinning the value is also strictly stronger
    than parity, because it catches both sides degrading the same way — a
    ``None``, a ``0.0``, or a ``NaN`` where an infinity was written — which is
    why there is no separate parity check for these.

    Crossed with every carrier because the JSON encoding of a non-finite float
    is chosen per container; one of them getting it wrong is exactly the kind
    of gap a node-properties-only test would miss.
    """
    expected = _NON_FINITE[key][1]
    read = _CARRIERS[carrier]
    for name, side in (
        ("local", non_finite_pair.local),
        ("remote", non_finite_pair.remote),
    ):
        got = read(side).get(key)
        where = f"{name}/{carrier}"
        assert isinstance(got, float), f"{where}: {key} read back as {got!r}"
        if math.isnan(expected):
            assert math.isnan(got), f"{where}: expected NaN for {key}, got {got!r}"
        else:
            assert (
                got == expected
            ), f"{where}: expected {expected} for {key}, got {got!r}"


# --- map key order ----------------------------------------------------------


def test_map_property_key_order_is_insertion_order(typed_pair):
    """A dict-valued property keeps the order it was written in, on both sides.

    Asserted per side against the written order rather than local-vs-remote:
    that is strictly stronger, since two sides re-keying the map the same way
    (alphabetically, say) would satisfy parity while still having lost the
    order. The keys are listed rather than compared as a dict because
    ``dict.__eq__`` ignores order — the key *list* is what carries it.
    """
    for side in (typed_pair.local, typed_pair.remote):
        got = list(side.node("a").properties.get("p_map").keys())
        assert got == list(_MAP), f"map key order changed: {got}"


def test_map_property_values_parity(typed_pair):
    assert_parity(typed_pair, lambda g: g.node("a").properties.get("p_map"))


# --- collection property views: the mapping protocol --------------------------


def test_collection_view_contains_parity(typed_pair):
    """`key in nodes.properties` answers the same on both sides."""
    assert_parity(typed_pair, lambda g: "p_u8" in g.nodes.properties)
    assert_parity(typed_pair, lambda g: "nope" in g.nodes.properties)


def test_collection_view_getitem_parity(typed_pair):
    """`nodes.properties[key]` returns the column, and raises `KeyError` for an
    unregistered key, on both sides — contrast `.get`, which returns `None`."""
    assert_parity(typed_pair, lambda g: list(g.nodes.properties["p_u8"]))
    assert_parity(typed_pair, lambda g: g.nodes.properties["nope"])


def test_collection_view_iter_parity(typed_pair):
    """`for k in nodes.properties` yields the keys on both sides.

    The metadata view is asserted too: it shares the macro-generated protocol
    remotely, but a regression could split the two, and iteration over an
    *empty* view (nothing in this build writes node metadata) is its own edge
    case worth holding to parity.
    """
    assert_parity(typed_pair, lambda g: sorted(g.nodes.properties))
    assert_parity(typed_pair, lambda g: sorted(g.nodes.metadata))
