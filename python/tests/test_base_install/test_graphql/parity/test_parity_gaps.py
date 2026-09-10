"""Executable divergence ledger: one strict-xfail case per known local↔remote gap.

Each case runs an API that exists locally but not (yet) on remote through the
same comparator. It is expected to fail today — local returns, remote raises,
so exception-parity trips. ``strict=True`` means the day remote implements the
API the case will XPASS and the suite goes RED, forcing the gap to be removed
from ``KNOWN_GAPS`` here and in ``_parity.py``. The ledger cannot silently rot.
"""

import pytest

from _parity import KNOWN_GAPS, assert_parity, graph_pair


def _build_basic(g):
    g.add_node(1, "a")
    g.add_node(2, "b")
    g.add_edge(3, "a", "b")


@pytest.fixture(scope="module")
def basic_pair():
    with graph_pair(_build_basic) as pair:
        yield pair


# (gap_key, fn) — gap_key must exist in KNOWN_GAPS. Each fn uses a local API that
# has no remote equivalent yet.
GAP_CASES = [
    ("nodes.history", lambda g: list(g.nodes.history)),
    ("edges.history", lambda g: list(g.edges.history)),
    ("edges.deletions", lambda g: list(g.edges.deletions)),
    ("expanding", lambda g: [w.count_edges() for w in g.expanding(1)]),
    ("rolling", lambda g: [w.count_edges() for w in g.rolling(2)]),
    (
        "collection_props.temporal",
        lambda g: sorted(g.nodes.properties.temporal.keys()),
    ),
    (
        "history.merge",
        lambda g: [t.t for t in g.node("a").history.merge(g.node("b").history)],
    ),
]


@pytest.mark.parametrize(
    "key,fn",
    [
        pytest.param(
            key,
            fn,
            marks=pytest.mark.xfail(reason=KNOWN_GAPS[key], strict=True),
            id=key,
        )
        for key, fn in GAP_CASES
    ],
)
def test_known_gap(basic_pair, key, fn):
    assert_parity(basic_pair, fn)


def test_gap_cases_are_all_ledgered():
    """Every executable gap case must correspond to a KNOWN_GAPS entry."""
    for key, _ in GAP_CASES:
        assert key in KNOWN_GAPS, f"gap case {key!r} missing from KNOWN_GAPS ledger"
