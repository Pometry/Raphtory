"""Prop-type behaviour matrix — comparison (in filters) and aggregation across every Prop type.

Python surfaces only three numeric kinds: every integer width (`u8`…`i64`) is `int`, `f32`/`f64` is
`float`, and `Decimal` is `decimal.Decimal` — so assertions check value plus that coarse kind, which
is all Python can observe.

Comparison families: all numeric types (integers, floats, Decimal) compare with one another by
magnitude (`5 == 5.0 == Decimal(5)`); every non-numeric type compares only within its own type, and
any cross-family comparison raises rather than silently returning no match.
"""

import datetime
from decimal import Decimal

import pytest

from raphtory import Graph, Prop, filter

# Single source of truth: one constructor per Prop type.
INT_TYPES = {
    "u8": Prop.u8,
    "u16": Prop.u16,
    "u32": Prop.u32,
    "u64": Prop.u64,
    "i32": Prop.i32,
    "i64": Prop.i64,
}
FLOAT_TYPES = {"f32": Prop.f32, "f64": Prop.f64}
DECIMAL_TYPES = {"decimal": lambda x: Prop.decimal(Decimal(x))}
NUMERIC = {**INT_TYPES, **FLOAT_TYPES, **DECIMAL_TYPES}

# Non-numeric types. Each constructor takes an int seed so the matrix can build one value per type
# uniformly; each compares only within its own type.
NON_NUMERIC = {
    "str": lambda x: Prop.str(str(x)),
    "bool": lambda x: Prop.bool(x != 0),
    "list": lambda x: Prop.list([Prop.i64(x)]),
    "map": lambda x: Prop.map({"k": x}),
    "ndtime": lambda x: Prop.naive_datetime(datetime.datetime(2020, 1, 1)),
}
ALL_TYPES = {**NUMERIC, **NON_NUMERIC}


def _py_kind(name):
    """The Python type a value of this numeric prop type surfaces as."""
    if name in INT_TYPES:
        return int
    if name in FLOAT_TYPES:
        return float
    return Decimal


def _family(name):
    """Comparison family: all numeric types share one; every other type is its own."""
    return "numeric" if name in NUMERIC else name


# --------------------------------------------------------------------------- #
# Comparison — family boundaries across EVERY type pair.
# --------------------------------------------------------------------------- #


def test_comparison_works_within_family_and_errors_across():
    """A comparison is valid only within a family: all numeric types with one another, and every
    other type only with its own. Any cross-family comparison (number vs string, string vs bool, …)
    raises rather than silently not matching."""
    g = Graph()
    g.add_node(0, "n", properties={f"v_{k}": c(5) for k, c in ALL_TYPES.items()})
    for s in ALL_TYPES:
        p = f"v_{s}"
        for f2, fc in ALL_TYPES.items():
            expr = filter.Node.property(p) == fc(5)
            if _family(s) == _family(f2):
                assert g.filter(expr).count_nodes() == 1, (
                    s,
                    f2,
                )  # equal value → matches
            else:
                with pytest.raises(Exception):
                    g.filter(expr).count_nodes()


def test_numeric_compares_by_magnitude_across_every_pair():
    """Within the numeric family every operator compares by magnitude regardless of the stored-type ×
    filter-type pair — including every pairing with Decimal."""
    g = Graph()
    g.add_node(0, "n", properties={f"v_{k}": c(5) for k, c in NUMERIC.items()})
    g.add_node(0, "bare", properties={})  # matches only is_none
    for sname in NUMERIC:
        p = f"v_{sname}"
        node = filter.Node.property(p)
        for fname, fc in NUMERIC.items():
            ctx = (sname, fname)
            assert g.filter(node == fc(5)).count_nodes() == 1, ctx
            assert g.filter(node != fc(5)).count_nodes() == 0, ctx
            assert g.filter(node < fc(6)).count_nodes() == 1, ctx
            assert g.filter(node < fc(5)).count_nodes() == 0, ctx
            assert g.filter(node <= fc(5)).count_nodes() == 1, ctx
            assert g.filter(node > fc(4)).count_nodes() == 1, ctx
            assert g.filter(node > fc(5)).count_nodes() == 0, ctx
            assert g.filter(node >= fc(5)).count_nodes() == 1, ctx
            assert g.filter(node.is_in([fc(5)])).count_nodes() == 1, ctx
            assert g.filter(node.is_not_in([fc(5)])).count_nodes() == 0, ctx
        assert g.filter(node.is_some()).count_nodes() == 1
        assert g.filter(node.is_none()).count_nodes() == 1  # only `bare`


def test_comparison_is_exact_beyond_float53():
    """Values above 2^53 differing by 1 are indistinguishable as f64 but must still compare
    correctly — proof that comparison widens through i128/Decimal, not f64."""
    big = 2**60
    g = Graph()
    g.add_node(0, "n", properties={"i": Prop.i64(big + 1)})
    node = filter.Node.property("i")
    # big+1 vs big: strictly greater and NOT equal (an f64 round-trip would call them equal).
    assert g.filter(node > Prop.i64(big)).count_nodes() == 1
    assert g.filter(node == Prop.i64(big)).count_nodes() == 0
    assert g.filter(node == Prop.i64(big + 1)).count_nodes() == 1
    # cross-type at the same magnitude: vs u64 and vs Decimal.
    assert g.filter(node > Prop.u64(big)).count_nodes() == 1
    assert g.filter(node > Prop.decimal(Decimal(big))).count_nodes() == 1
    assert g.filter(node == Prop.decimal(Decimal(big + 1))).count_nodes() == 1

    # Distinguish by 1 right at the u64 ceiling.
    umax = 2**64 - 1
    g.add_node(0, "m", properties={"w": Prop.u64(umax)})
    w = filter.Node.property("w")
    assert g.filter(w > Prop.u64(umax - 1)).count_nodes() == 1
    assert g.filter(w == Prop.u64(umax - 1)).count_nodes() == 0


def test_bool_ordering_and_membership():
    """Bools support the full operator set (`False < True`); comparing a bool against numbers via
    `is_in` simply doesn't match (mirroring `is_in([1, 2]) == 0`)."""
    g = Graph()
    for name, v in [("t1", True), ("t2", True), ("f1", False)]:
        g.add_node(0, name, properties={"active": Prop.bool(v)})
    active = filter.Node.property("active")
    assert g.filter(active == True).count_nodes() == 2
    assert g.filter(active != False).count_nodes() == 2
    assert g.filter(active.is_in([True])).count_nodes() == 2
    assert g.filter(active.is_in([True, False])).count_nodes() == 3
    assert g.filter(active.is_not_in([False])).count_nodes() == 2
    assert g.filter(active < True).count_nodes() == 1
    assert g.filter(active > False).count_nodes() == 2
    assert g.filter(active >= False).count_nodes() == 3
    assert g.filter(active <= False).count_nodes() == 1
    # NOTE (for team discussion): asymmetry — `active == 1` *raises* (bool is its own comparison
    # family, see test_comparison_works_within_family_and_errors_across), but `is_in` with numbers
    # silently returns no match instead of raising. Left as-is pending a decision on which wins.
    assert g.filter(active.is_in([1, 2])).count_nodes() == 0
    assert g.filter(active.is_some()).count_nodes() == 3
    assert g.filter(active.is_none()).count_nodes() == 0


def test_map_equality_in_filters():
    """Map properties compare for equality/inequality (order-independent); ordering (`<`) stays
    unsupported since maps aren't ordered."""
    g = Graph()
    g.add_node(0, "a", properties={"meta": Prop.map({"role": "eng", "level": 2})})
    g.add_node(0, "b", properties={"meta": Prop.map({"role": "mgr"})})
    meta = filter.Node.property("meta")
    assert g.filter(meta == {"role": "eng", "level": 2}).count_nodes() == 1
    assert (
        g.filter(meta == {"level": 2, "role": "eng"}).count_nodes() == 1
    )  # order-free
    assert g.filter(meta != {"role": "eng", "level": 2}).count_nodes() == 1


def test_is_in_mixes_numeric_types_in_one_set():
    """A single `is_in` set may mix numeric types; membership is by magnitude."""
    g = Graph()
    g.add_node(0, "n", properties={"v": Prop.i64(5)})
    v = filter.Node.property("v")
    assert (
        g.filter(
            v.is_in([Prop.u8(1), Prop.f64(5.0), Prop.decimal(Decimal(9))])
        ).count_nodes()
        == 1
    )
    assert (
        g.filter(
            v.is_in([Prop.u8(1), Prop.f32(2.0), Prop.decimal(Decimal(9))])
        ).count_nodes()
        == 0
    )
    assert g.filter(v.is_not_in([Prop.u8(1), Prop.f32(2.0)])).count_nodes() == 1


# --------------------------------------------------------------------------- #
# Aggregation across each numeric type.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name,ctor", list(NUMERIC.items()))
def test_aggregate_each_numeric_type(name, ctor):
    """Values [2, 4, 6] of a single type reduce to the expected scalars, surfacing as the right
    Python kind. `min`/`max`/`median` return `(time, value)` pairs."""
    g = Graph()
    for t, val in enumerate([2, 4, 6]):
        g.add_node(t, "a", properties={"x": ctor(val)})
    tp = g.node("a").properties.temporal.get("x")
    kind = _py_kind(name)

    total = tp.sum()
    assert total == 12 and isinstance(total, kind)
    # mean is always a float (an average is inherently fractional), for every numeric type.
    assert tp.mean() == 4.0 and isinstance(tp.mean(), float)
    # min / max / median return (time, value); the value keeps the stored type's Python kind.
    for agg, want in ((tp.min(), 2), (tp.max(), 6), (tp.median(), 4)):
        assert agg[1] == want and isinstance(agg[1], kind)
    assert tp.count() == 3


def test_aggregate_widens_and_spills_on_big_values():
    """Sums that outgrow their type widen exactly (`u8`→…), spilling to Decimal past `u64`/`i64`
    rather than wrapping or losing precision to f64."""
    g = Graph()
    for t in range(3):
        g.add_node(t, "a", properties={"x": Prop.u8(255)})
    assert (
        g.node("a").properties.temporal.get("x").sum() == 765
    )  # 3*255, widened past u8/u16

    g.add_node(0, "b", properties={"u32": Prop.u32(4294967295)})
    g.add_node(1, "b", properties={"u32": Prop.u32(4294967295)})
    assert g.node("b").properties.temporal.get("u32").sum() == 8589934590  # u32 → u64

    g.add_node(0, "c", properties={"i32": Prop.i32(2147483647)})
    g.add_node(1, "c", properties={"i32": Prop.i32(2147483647)})
    assert g.node("c").properties.temporal.get("i32").sum() == 4294967294  # i32 → i64

    umax = 2**64 - 1
    for t in range(3):
        g.add_node(t, "d", properties={"u64": Prop.u64(umax)})
    s = g.node("d").properties.temporal.get("u64").sum()
    assert s == 3 * umax and isinstance(
        s, Decimal
    )  # spilled to exact Decimal, not a rounded float

    imax = 2**63 - 1
    for t in range(3):
        g.add_node(t, "e", properties={"i64": Prop.i64(imax)})
    si = g.node("e").properties.temporal.get("i64").sum()
    assert si == 3 * imax and isinstance(si, Decimal)
