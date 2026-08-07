"""Smoke coverage for the local↔remote parity harness.

A representative slice of read APIs run through the same ``build`` on both a
local ``Graph`` and a ``RemoteGraph`` and are asserted equal via the shared
comparator. This is the foundation the full per-API differential suite builds
on — not exhaustive coverage yet.
"""

import pytest

from _parity import assert_parity, graph_pair


def _build_basic(g):
    """A tiny graph on the shared drop-in surface: two nodes and one edge."""
    g.add_node(1, "ben")
    g.add_node(2, "hamza")
    g.add_edge(3, "ben", "hamza")


@pytest.fixture(scope="module")
def basic_pair():
    # One server for all read cases in this module (reads don't mutate).
    with graph_pair(_build_basic) as pair:
        yield pair


# (id, fn) — each fn takes a graph handle and returns a comparable result.
GRAPH_READS = [
    ("count_nodes", lambda g: g.count_nodes()),
    ("count_edges", lambda g: g.count_edges()),
    ("node_names", lambda g: [n.name for n in g.nodes]),
    ("edge_pairs", lambda g: [(e.src.name, e.dst.name) for e in g.edges]),
    ("has_node_present", lambda g: g.has_node("ben")),
    ("has_node_absent", lambda g: g.has_node("nobody")),
    ("ben_degree", lambda g: g.node("ben").degree()),
    ("node_absent_is_none", lambda g: g.node("nobody")),
    ("window_edge_count", lambda g: g.window(0, 5).edges.count()),
    ("before_excludes", lambda g: g.before(3).edges.count()),
    ("after_excludes", lambda g: g.after(3).edges.count()),
    ("at_includes", lambda g: g.at(3).edges.count()),
]


@pytest.mark.parametrize("name,fn", GRAPH_READS, ids=[c[0] for c in GRAPH_READS])
def test_graph_read_parity(basic_pair, name, fn):
    assert_parity(basic_pair, fn)
