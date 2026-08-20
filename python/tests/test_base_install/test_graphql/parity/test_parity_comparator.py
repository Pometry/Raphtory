"""What the parity comparator is and is not allowed to reconcile.

``canonical`` is the load-bearing contract of this suite: every other module
trusts it to turn "local == remote" into a meaningful claim. A comparator that
smooths too much makes the whole suite vacuous — it would pass while the wire
format mangled floats, re-keyed maps or reordered histories — so its policy is
pinned here rather than left to the docstring.

The policy: it may bridge only the ways the two sides are unavoidably *different
objects over different graphs* — entity identity, and materializing containers
whose classes differ. It reorders nothing. Every actual difference in an answer
must survive to the assertion. The negative cases below are the important half.
"""

import datetime
import math

import pytest

from _parity import GraphPair, assert_parity, canonical, graph_pair


def _pair(local, remote):
    """A pair whose "graphs" are plain values, so ``fn`` is the identity."""
    return GraphPair(local=local, remote=remote)


def _agree(local, remote):
    """True if the comparator considers these two results equal."""
    return canonical(local) == canonical(remote)


# --- allowed: the two sides are different objects ---------------------------


def _build(g):
    g.add_node(1, "a", properties={"score": 1.5})
    g.add_node(2, "b")
    g.add_node(3, "c")
    g.add_edge(4, "a", "b")
    g.add_edge(5, "a", "c")


@pytest.fixture(scope="module")
def pair():
    with graph_pair(_build) as p:
        yield p


def test_entities_reconcile_to_identity(pair):
    """A ``Node``/``RemoteNode`` for the same node compares equal.

    They are objects over different graphs, so without this reduction nothing
    entity-shaped could ever be compared.
    """
    assert _agree(pair.local.node("a"), pair.remote.node("a"))
    assert _agree(pair.local.edge("a", "b"), pair.remote.edge("a", "b"))


def test_distinct_entities_still_differ(pair):
    """Identity reduction must not collapse *different* entities together."""
    assert not _agree(pair.local.node("a"), pair.remote.node("b"))
    assert not _agree(pair.local.edge("a", "b"), pair.remote.edge("a", "c"))


def test_entity_collections_compare_by_contents(pair):
    """A local collection and its remote twin are different classes, so they are
    listed to make their contents comparable — without reordering either side.

    The comparator used to sort these, on the grounds that iteration order is
    unspecified. It does not any more: the two sides agree on order in practice,
    so leaving it alone means a future ordering divergence fails here instead of
    being absorbed."""
    assert _agree(pair.local.nodes, pair.remote.nodes)
    assert _agree(pair.local.edges, pair.remote.edges)
    assert _agree(pair.local.node("a").neighbours, pair.remote.node("a").neighbours)


def test_entity_collections_of_different_membership_differ(pair):
    """Materializing a collection must not hide a membership difference."""
    assert not _agree(pair.local.nodes, pair.remote.node("a").neighbours)


def test_entity_collection_order_is_not_absorbed():
    """A reordered collection is a difference, not a detail.

    Uses stand-ins rather than real handles because the point is the
    comparator's policy: two collections with the same members in a different
    order must not agree."""

    class Coll:
        def __init__(self, names):
            self._names = names

        def __iter__(self):
            return iter(self._names)

    assert _agree(Coll(["a", "b", "c"]), Coll(["a", "b", "c"]))
    assert not _agree(Coll(["a", "b", "c"]), Coll(["c", "b", "a"]))


# --- refused: anything that is a real difference in the answer --------------


def test_float_precision_is_not_smoothed():
    """No rounding. A perturbed round-trip has to fail, not compare equal."""
    assert not _agree(0.1 + 0.2, 0.3)
    assert not _agree(1.0000000001, 1.0)
    assert _agree(1.5, 1.5)


def test_nan_is_not_folded_to_a_sentinel():
    """``NaN`` is left alone, which is why non-finite floats are asserted
    explicitly per side in ``test_parity_props`` instead of by parity."""
    assert not _agree(math.nan, math.nan)


def test_datetime_timezone_is_not_normalized():
    """A naive and an aware datetime for the same instant are not the same
    answer, so the comparator must not reduce both to an epoch float."""
    naive = datetime.datetime(2021, 3, 4, 5, 6, 7)
    aware = datetime.datetime(2021, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)
    assert not _agree(naive, aware)
    assert _agree(aware, aware)
    assert _agree(naive, naive)


def test_sequence_order_is_preserved():
    """Nothing is reordered: order is part of the answer everywhere (histories,
    list properties, sorts, and entity collections alike)."""
    assert not _agree([1, 2, 3], [3, 2, 1])
    assert not _agree(("a", "b"), ("b", "a"))
    assert _agree([1, 2, 3], [1, 2, 3])


def test_non_entity_iterables_are_materialized_without_reordering():
    """A local ``History`` and a remote ``RemoteHistory`` are distinct classes,
    so they are listed to make their contents comparable — in order."""

    class Localish:
        def __init__(self, items):
            self._items = items

        def __iter__(self):
            return iter(self._items)

    class Remoteish:
        def __init__(self, items):
            self._items = items

        def __iter__(self):
            return iter(self._items)

    assert _agree(Localish([1, 2, 3]), Remoteish([1, 2, 3]))
    assert not _agree(Localish([1, 2, 3]), Remoteish([3, 2, 1]))


def test_map_values_must_match_but_key_order_need_not():
    """``dict.__eq__`` already ignores key order, so keys are left alone; a
    differing *value* still has to fail. Map insertion order is a product
    guarantee asserted explicitly in ``test_parity_props``."""
    assert _agree({"a": 1, "b": 2}, {"b": 2, "a": 1})
    assert not _agree({"a": 1, "b": 2}, {"a": 1, "b": 3})
    assert not _agree({"a": 1}, {"a": 1, "b": 2})


def test_scalar_types_are_not_coerced():
    """No cross-type leniency: a stringified number is not the number."""
    assert not _agree(1, "1")
    assert not _agree(1.0, "1.0")
    assert not _agree(None, 0)
    assert not _agree(True, 1.5)


def test_strings_are_not_exploded_into_characters():
    """Strings are iterable; the comparator must treat them as scalars."""
    assert canonical("abc") == "abc"
    assert not _agree("abc", ["a", "b", "c"])


# --- the assertion wrapper itself -------------------------------------------


def test_assert_parity_fails_on_a_real_difference():
    with pytest.raises(AssertionError, match="value parity mismatch"):
        assert_parity(_pair(1, 2), lambda g: g)


def test_assert_parity_requires_both_sides_to_raise_the_same_type():
    def boom(g):
        if g == "local":
            raise ValueError("local only")
        return 1

    with pytest.raises(AssertionError, match="exception parity mismatch"):
        assert_parity(_pair("local", "remote"), boom)


def test_assert_parity_rejects_two_different_exception_types():
    """Both sides refusing is only parity if they refuse the same way — a
    remote that turns every local ValueError into a generic error would
    otherwise pass every rejection case."""

    def boom(g):
        if g == "local":
            raise ValueError("local flavour")
        raise TypeError("remote flavour")

    with pytest.raises(AssertionError, match="exception parity mismatch"):
        assert_parity(_pair("local", "remote"), boom)


def test_assert_parity_accepts_the_same_exception_on_both_sides():
    def boom(g):
        raise ValueError("both")

    assert_parity(_pair("local", "remote"), boom)
