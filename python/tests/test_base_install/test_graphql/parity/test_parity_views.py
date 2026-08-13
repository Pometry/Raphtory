"""View-op parity matrix: every view operation, on every handle that exposes it.

A view op (``window``, ``at``, ``layer``, ``shrink_start``, …) is pure plumbing
on the local ``Graph``: it rewrites the view a handle answers through, in
process. On ``RemoteGraph`` the same call has to be re-encoded and replayed on
the server, so an op can be silently dropped, attached to the wrong handle, or
replayed with off-by-one bounds and *still* return a plausible-looking answer.
That is the blind spot this module closes: it crosses every view op with every
handle type that exposes it and asserts both sides answer identically.

Two things are compared for each ``(handle, op)`` pair:

* the **content** the viewed handle reports — a per-handle probe over names,
  edge pairs, degrees, earliest/latest time, layer names, history and edge
  validity, and
* the **window bounds** the view installs — ``start`` / ``end`` /
  ``window_size``, which are observable on every handle type.

Every pair is also guarded against a *vacuous* pass: the op must demonstrably
change what the handle reports versus the same handle unviewed, on both sides.
Without that guard a remote that ignored every view op would pass the entire
matrix.
"""

import itertools

import pytest

from _parity import assert_parity, canonical, graph_pair


def _build_matrix(g):
    """One graph on which *every* view op is observable.

    Shaped so no op in the matrix is a no-op:

    * events at t=1..9 plus a deletion at t=11, so the temporal ops
      (``window`` / ``at`` / ``before`` / ``after`` / ``latest`` / ``shrink_*``)
      each land on a different slice;
    * four layers — the implicit ``_default`` plus ``knows``, ``works``,
      ``likes`` — so the layer ops each select a different edge set, and
      ``default_layer()`` is neither empty nor everything;
    * ``a -> b`` deliberately lives in *two* layers (``_default`` and ``knows``)
      at three different times, so the ``Edge`` handle — which cannot lose
      members the way a collection can — still changes under every op;
    * node types and a numeric ``score`` property, so type- and property-shaped
      reads are exercised alongside topology.
    """
    g.add_node(1, "a", node_type="zebra", properties={"score": 3.0})
    g.add_node(1, "b", node_type="ant", properties={"score": 1.0})
    g.add_node(2, "c", node_type="mole", properties={"score": 2.0})
    g.add_node(6, "d", node_type="ant", properties={"score": 4.0})
    g.add_edge(2, "a", "b")
    g.add_edge(3, "a", "b", layer="knows")
    g.add_edge(7, "a", "b", layer="knows")
    g.add_edge(3, "b", "c", layer="knows")
    g.add_edge(4, "c", "a", layer="works")
    g.add_edge(5, "a", "c", layer="knows")
    g.add_edge(6, "c", "d", layer="works")
    g.add_edge(8, "d", "a")
    g.add_edge(9, "b", "d", layer="likes")
    g.delete_edge(11, "a", "b", "knows", None)


@pytest.fixture(scope="module")
def matrix_pair():
    # One server for the whole matrix — every case below is read-only.
    with graph_pair(_build_matrix) as pair:
        yield pair


# --- probes -----------------------------------------------------------------
#
# A probe reduces a viewed handle to a comparable value. Reads are chosen to be
# sensitive to *both* axes a view op moves: time (earliest/latest/history) and
# layer (layer names, degrees, edge membership). Identity-only reads are not
# enough — a collection view op keeps its members and narrows what each member
# answers, so `sorted(n.name for n in nodes)` is invariant under every op.


def _node_facts(n):
    return (
        n.name,
        n.degree(),
        n.in_degree(),
        n.out_degree(),
        n.earliest_time,
        n.latest_time,
    )


def _edge_facts(e):
    return (
        e.src.name,
        e.dst.name,
        e.earliest_time,
        e.latest_time,
        tuple(sorted(e.layer_names)),
        e.is_valid(),
        e.is_deleted(),
    )


def _probe_graph(h):
    return (
        h.count_nodes(),
        h.count_edges(),
        sorted(_node_facts(n) for n in h.nodes),
        sorted(_edge_facts(e) for e in h.edges),
        h.earliest_time,
        h.latest_time,
    )


def _probe_node(h):
    return (
        _node_facts(h),
        sorted(x.name for x in h.neighbours),
        sorted(x for x in h.history),
    )


def _probe_edge(h):
    return None if h is None else (_edge_facts(h), sorted(x for x in h.history))


def _probe_nodes(h):
    return sorted(_node_facts(n) for n in h)


def _probe_edges(h):
    return sorted(_edge_facts(e) for e in h)


def _probe_path_from_node(h):
    return sorted(_node_facts(n) for n in h)


def _probe_path_from_graph(h):
    # Read through `collect()`, whose shape is the flat per-source node lists.
    # Iteration yields `(source, path)` pairs on both sides — asserted
    # separately by `test_path_from_graph_iteration_yields_pairs`.
    return sorted(sorted(_node_facts(n) for n in sub) for sub in h.collect())


def _probe_nested_edges(h):
    return sorted(sorted(_edge_facts(e) for e in sub) for sub in h)


def _probe_bounds(h):
    """The window a view installs — observable on every handle type."""
    return (h.start, h.end, h.window_size)


# handle name -> (how to reach it from a graph handle, content probe)
HANDLES = {
    "graph": (lambda g: g, _probe_graph),
    "node": (lambda g: g.node("a"), _probe_node),
    "edge": (lambda g: g.edge("a", "b"), _probe_edge),
    "nodes": (lambda g: g.nodes, _probe_nodes),
    "edges": (lambda g: g.edges, _probe_edges),
    "path_from_node": (lambda g: g.node("a").neighbours, _probe_path_from_node),
    "path_from_graph": (lambda g: g.nodes.neighbours, _probe_path_from_graph),
    "nested_edges": (lambda g: g.nodes.edges, _probe_nested_edges),
}

# view op name -> how to apply it. Arguments are picked so the op bites on
# `_build_matrix` (see `test_view_op_narrows`, which enforces exactly that).
VIEW_OPS = {
    "window": lambda h: h.window(2, 6),
    "at": lambda h: h.at(3),
    "before": lambda h: h.before(5),
    "after": lambda h: h.after(4),
    "latest": lambda h: h.latest(),
    "snapshot_at": lambda h: h.snapshot_at(5),
    "snapshot_latest": lambda h: h.snapshot_latest(),
    "layer": lambda h: h.layer("knows"),
    "layers": lambda h: h.layers(["knows", "works"]),
    "exclude_layer": lambda h: h.exclude_layer("knows"),
    "exclude_layers": lambda h: h.exclude_layers(["knows", "works"]),
    "valid_layers": lambda h: h.valid_layers(["knows"]),
    "exclude_valid_layer": lambda h: h.exclude_valid_layer("knows"),
    "exclude_valid_layers": lambda h: h.exclude_valid_layers(["knows", "works"]),
    "default_layer": lambda h: h.default_layer(),
    "shrink_window": lambda h: h.shrink_window(3, 8),
    "shrink_start": lambda h: h.shrink_start(4),
    "shrink_end": lambda h: h.shrink_end(6),
}

# `snapshot_latest()` cannot change *content* on an event graph: deletions are
# recorded but never remove events, so "everything not deleted at the latest
# time" is everything. Its one observable effect is the upper bound it installs,
# so its non-vacuity is proven against `_probe_bounds` instead. Making it narrow
# by content would need a PersistentGraph pair, which the shared `graph_pair`
# fixture does not build.
_BOUNDS_ONLY_NARROWING = {"snapshot_latest"}

MATRIX = list(itertools.product(sorted(HANDLES), sorted(VIEW_OPS)))
_MATRIX_IDS = [f"{handle}-{op}" for handle, op in MATRIX]


# --- parity -----------------------------------------------------------------


@pytest.mark.parametrize("handle,op", MATRIX, ids=_MATRIX_IDS)
def test_view_op_parity(matrix_pair, handle, op):
    """Local and remote agree on what a handle reports through a given view."""
    reach, probe = HANDLES[handle]
    apply_op = VIEW_OPS[op]

    assert_parity(matrix_pair, lambda g: probe(apply_op(reach(g))))
    assert_parity(matrix_pair, lambda g: _probe_bounds(apply_op(reach(g))))


# --- non-vacuity guard ------------------------------------------------------

# Unviewed probe values, memoized per (handle, side): the matrix asks for them
# once per op and they cost a round trip on the remote side.
_BASELINES = {}


def _baseline(pair, handle, probe_name):
    key = (handle, probe_name)
    if key not in _BASELINES:
        reach, probe = HANDLES[handle]
        probe = probe if probe_name == "content" else _probe_bounds
        _BASELINES[key] = {
            "local": canonical(probe(reach(pair.local))),
            "remote": canonical(probe(reach(pair.remote))),
        }
    return _BASELINES[key]


@pytest.mark.parametrize("handle,op", MATRIX, ids=_MATRIX_IDS)
def test_view_op_narrows(matrix_pair, handle, op):
    """The op must change the answer — otherwise the parity above is vacuous.

    Asserted per side, not across sides: two graphs that both ignore a view op
    agree with each other, so cross-side equality alone proves nothing.
    """
    reach, probe = HANDLES[handle]
    apply_op = VIEW_OPS[op]
    probe_name = "bounds" if op in _BOUNDS_ONLY_NARROWING else "content"
    if probe_name == "bounds":
        probe = _probe_bounds
    baseline = _baseline(matrix_pair, handle, probe_name)

    for side_name, side in (
        ("local", matrix_pair.local),
        ("remote", matrix_pair.remote),
    ):
        viewed = canonical(probe(apply_op(reach(side))))
        assert viewed != baseline[side_name], (
            f"{side_name} {handle}.{op}() did not change the {probe_name} probe: "
            f"the view op was a no-op, so its parity case is vacuous"
        )


def test_matrix_covers_every_handle_and_op():
    """The matrix is the full cross product — no handle or op quietly dropped."""
    assert sorted(HANDLES) == [
        "edge",
        "edges",
        "graph",
        "nested_edges",
        "node",
        "nodes",
        "path_from_graph",
        "path_from_node",
    ]
    assert sorted(VIEW_OPS) == [
        "after",
        "at",
        "before",
        "default_layer",
        "exclude_layer",
        "exclude_layers",
        "exclude_valid_layer",
        "exclude_valid_layers",
        "latest",
        "layer",
        "layers",
        "shrink_end",
        "shrink_start",
        "shrink_window",
        "snapshot_at",
        "snapshot_latest",
        "valid_layers",
        "window",
    ]
    assert len(MATRIX) == len(HANDLES) * len(VIEW_OPS)


# --- iteration shape --------------------------------------------------------


# Ways to reach a nested node path. Each pairs sources with paths, and each puts
# a different chain between the source collection and the path — the remote has
# to re-root every one of them at a single source to name the pair.
NESTED_PATHS = {
    "neighbours": lambda g: g.nodes.neighbours,
    "in_neighbours": lambda g: g.nodes.in_neighbours,
    "out_neighbours": lambda g: g.nodes.out_neighbours,
    # A view op applied to the sources, before the hop.
    "windowed_sources": lambda g: g.nodes.window(2, 6).neighbours,
    # A view op applied to the paths, after the hop.
    "windowed_paths": lambda g: g.nodes.neighbours.window(2, 6),
    # Membership narrowing on each side of the hop: below it picks the sources,
    # above it picks the path members.
    "type_filtered_sources": lambda g: g.nodes.type_filter(["ant"]).neighbours,
    "type_filtered_paths": lambda g: g.nodes.neighbours.type_filter(["ant"]),
    "two_hop": lambda g: g.nodes.neighbours.neighbours,
    # Reached through a nested edge collection rather than a node traversal.
    "edge_endpoints": lambda g: g.nodes.edges.src,
}


@pytest.mark.parametrize("reach", NESTED_PATHS.values(), ids=list(NESTED_PATHS))
def test_path_from_graph_iteration_yields_pairs(matrix_pair, reach):
    """Iterating a PathFromGraph yields ``(source, path)`` pairs on both sides.

    The source node is what makes the pairing usable — without it a row is just
    an anonymous node list, and (worse) a two-element row would unpack into the
    wrong variables without complaint. Compared pair by pair: the source's name
    and the names of the nodes on that source's path.
    """
    assert_parity(
        matrix_pair,
        lambda g: sorted(
            (source.name, tuple(sorted(n.name for n in path)))
            for source, path in reach(g)
        ),
    )


def test_path_from_graph_iteration_yields_chainable_paths(matrix_pair):
    """The yielded path is a live handle, not a materialized list.

    It must keep composing — a view op applied to it, and a read taken from it,
    have to answer the same as they do locally. That is what rules out
    "iteration returns a snapshot" as an implementation.
    """
    assert_parity(
        matrix_pair,
        lambda g: sorted(
            (source.name, tuple(sorted(n.name for n in path.window(2, 6))))
            for source, path in g.nodes.neighbours
        ),
    )
    assert_parity(
        matrix_pair,
        lambda g: sorted(
            (source.name, tuple(sorted(path.degree())))
            for source, path in g.nodes.neighbours
        ),
    )


def test_delete_edge_takes_an_event_id_on_both_sides():
    """`delete_edge` accepts the same arguments — ``event_id`` included — on both.

    The signatures used to diverge (remote had no ``event_id`` at all), so a
    graph-agnostic build could not call it. One call form now works on both, and
    the tombstone it records — timestamp *and* event id — must match. Built on
    its own pair rather than the module fixture, because it writes.
    """

    def build(g):
        g.add_node(1, "a")
        g.add_node(1, "b")
        g.add_edge(2, "a", "b", layer="knows")
        g.delete_edge(5, "a", "b", layer="knows", event_id=7)

    with graph_pair(build) as pair:
        assert_parity(pair, lambda g: g.edge("a", "b").is_deleted())
        assert_parity(
            pair,
            lambda g: sorted(
                (t.t, t.event_id) for t in g.edge("a", "b").layer("knows").deletions
            ),
        )
        assert_parity(pair, lambda g: g.count_edges())
