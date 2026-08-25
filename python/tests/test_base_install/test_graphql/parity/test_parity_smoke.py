"""Smoke coverage for the local↔remote parity harness.

A representative slice of read APIs run through the same ``build`` on both a
local ``Graph`` and a ``RemoteGraph`` and are asserted equal via the shared
comparator. This is the foundation the full per-API differential suite builds
on — not exhaustive coverage yet.
"""

import pytest

from _parity import GRAPH_TYPES, assert_parity, graph_pair


def _build_basic(g):
    """A tiny graph on the shared drop-in surface: two nodes and one edge."""
    g.add_node(1, "ben")
    g.add_node(2, "hamza")
    g.add_edge(3, "ben", "hamza")


@pytest.fixture(scope="module", params=GRAPH_TYPES)
def basic_pair(request):
    # One server per graph model for all read cases in this module (reads don't
    # mutate). Running both models keeps `graph_type` honest: the reads below
    # are model-independent, so a pair built from mismatched halves — an EVENT
    # local graph against a PERSISTENT remote — would show up here.
    with graph_pair(_build_basic, graph_type=request.param) as pair:
        yield pair


@pytest.mark.parametrize(
    "graph_type,edges_after_last_event", [("EVENT", 0), ("PERSISTENT", 1)]
)
def test_graph_type_selects_the_model_on_both_sides(graph_type, edges_after_last_event):
    """``graph_type`` reaches *both* halves of the pair, not just the local one.

    Asserted through behaviour rather than types, because the remote handle is a
    ``RemoteGraph`` for either model — there is no client-side class to inspect,
    so only a model-dependent answer can show which graph the server built. An
    edge added at t=3 is still present at t=100 in a persistent graph and gone
    in an event graph, which pins each side to the requested model: a pair built
    from mismatched halves fails the parity assert, and a pair that silently
    built two EVENT graphs fails the expected count.
    """
    with graph_pair(_build_basic, graph_type=graph_type) as pair:
        assert_parity(pair, lambda g: g.at(100).count_edges())
        for name, side in (("local", pair.local), ("remote", pair.remote)):
            assert side.at(100).count_edges() == edges_after_last_event, (
                f"{name}: {graph_type} graph reported "
                f"{side.at(100).count_edges()} edges at t=100"
            )


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
    ("window_edge_count", lambda g: len(g.window(0, 5).edges)),
    ("before_excludes", lambda g: len(g.before(3).edges)),
    ("after_excludes", lambda g: len(g.after(3).edges)),
    ("at_includes", lambda g: len(g.at(3).edges)),
]


@pytest.mark.parametrize("name,fn", GRAPH_READS, ids=[c[0] for c in GRAPH_READS])
def test_graph_read_parity(basic_pair, name, fn):
    assert_parity(basic_pair, fn)
