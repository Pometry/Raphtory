"""Edge-collection filtering across every filter type and combination.

Singles are checked against the graph-level filter and the chained-view references. Combinations
are checked against set algebra over the single-filter results, question by question: a filter
answers "which nodes stay" and "which edges stay", a node predicate answers the second by "both
ends stayed", and `&`, `|`, `~` combine the direct answers. Within one kind that is plain set
algebra (`~` of an edge predicate keeps the other edges; `~` of a node predicate keeps the nodes
that fail it and the edges between them; `~(a & b)` is `~a | ~b`, `~(a | b)` is `~a & ~b`).
Across kinds, `&` intersects, `~(a & b)` negates each answer, and `|` leaves both questions open
(every edge), as the graph filter has always done; that last reading is inherited, not chosen here.
"""

from itertools import combinations

import pytest

from raphtory import filter
from utils import with_variants

Graph = filter.Graph
Node = filter.Node
Edge = filter.Edge

TIME_VIEWS = {"before", "after", "window", "at", "latest", "snap_at", "snap_latest"}
VIEWS = TIME_VIEWS | {"layer", "layers2"}
NODE_KIND = {"node_prop", "node_name"}


def _init(graph):
    graph.add_node(5, "a", {"score": 10})
    graph.add_node(10, "b", {"score": 20})
    graph.add_node(15, "c", {"score": 30})
    graph.add_edge(5, "a", "b", {"weight": 3}, layer="work")
    graph.add_edge(10, "b", "c", {"weight": 8}, layer="work")
    graph.add_edge(15, "c", "a", {"weight": 20}, layer="friends")
    graph.add_edge(12, "d", "d", {"weight": 4})
    graph.delete_edge(20, "a", "b", layer="work")
    return graph


def _atoms():
    return {
        "edge_prop": Edge.property("weight") > 5,
        "src": Edge.src().name() == "a",
        "dst": Edge.dst().name() == "c",
        "node_prop": Node.property("score") > 15,
        "node_name": Node.name().is_in(["b", "c"]),
        "layer": Graph.layer("work"),
        "layers2": Graph.layers(["work", "friends"]),
        "before": Graph.before(10),
        "after": Graph.after(8),
        "window": Graph.window(3, 12),
        "at": Graph.at(10),
        "latest": Graph.latest(),
        "snap_at": Graph.snapshot_at(10),
        "snap_latest": Graph.snapshot_latest(),
        "is_valid": Edge.is_valid(),
        "is_deleted": Edge.is_deleted(),
        "is_active": Edge.is_active(),
        "self_loop": Edge.is_self_loop(),
    }


def _kind(name):
    return "view" if name in VIEWS else ("node" if name in NODE_KIND else "edge")


def _and_is_broken(a, b):
    # `view & X` is not set algebra: the view applies first and `X` runs inside it
    # (`test_a_view_applies_first_under_and`), so it has no set-derived expectation here.
    return a in VIEWS or b in VIEWS


def _or_is_refused(a, b):
    # A view under `|` has no meaning the engine can give it, so it is refused when written
    # (`test_a_view_under_or_or_not_is_refused`).
    return a in VIEWS or b in VIEWS


def _not_is_refused(a):
    return a in VIEWS


def _ids(collection):
    return frozenset(e.id for e in collection)


def _view_references(graph):
    """Each view atom spelled as the equivalent chained view."""
    return {
        "layer": graph.layers(["work"]),
        "layers2": graph.layers(["work", "friends"]),
        "before": graph.before(10),
        "after": graph.after(8),
        "window": graph.window(3, 12),
        "at": graph.at(10),
        "latest": graph.latest(),
        "snap_at": graph.snapshot_at(10),
        "snap_latest": graph.snapshot_latest(),
    }


# What each non-view atom selects, evaluated directly over the collection. Node
# filters keep the edges whose *both* endpoints pass, which is how a node filter
# reduces onto an edge.
def _node_scores(graph, threshold):
    return {
        node.name
        for node in graph.nodes
        if (node.properties.get("score") or 0) > threshold
    }


def _both_endpoints(graph, names):
    return {
        edge.id
        for edge in graph.edges
        if edge.src.name in names and edge.dst.name in names
    }


def _all_names(graph):
    return {node.name for node in graph.nodes}


def _node_sets(graph):
    """The node set each node-kind atom keeps."""
    return {
        "node_prop": _node_scores(graph, 15),
        "node_name": {"b", "c"},
    }


def _predicate_references(graph):
    return {
        "edge_prop": {
            e.id for e in graph.edges if (e.properties.get("weight") or 0) > 5
        },
        "src": {e.id for e in graph.edges if e.src.name == "a"},
        "dst": {e.id for e in graph.edges if e.dst.name == "c"},
        "node_prop": _both_endpoints(graph, _node_sets(graph)["node_prop"]),
        "node_name": _both_endpoints(graph, _node_sets(graph)["node_name"]),
        "is_valid": {e.id for e in graph.edges if e.is_valid()},
        "is_deleted": {e.id for e in graph.edges if e.is_deleted()},
        "is_active": {e.id for e in graph.edges if e.is_active()},
        "self_loop": {e.id for e in graph.edges if e.src.name == e.dst.name},
    }


def _singles(graph):
    """What each atom selects, computed *without* the subscript under test.

    Reading these back through `graph.edges[atom]` would make the expectations
    below agree with the thing they are meant to check: where a single filter
    fails open, `EVERYTHING & X == X`, so a combination that dropped a term
    matches its expectation and the pins report a live bug as fixed. Views are
    referenced through the equivalent chained view instead, and predicates are
    evaluated over the collection directly.
    """
    views = _view_references(graph)
    singles = {
        name: frozenset(ids) for name, ids in _predicate_references(graph).items()
    }
    singles.update({name: _ids(view.edges) for name, view in views.items()})
    missing = set(_atoms()) - set(singles)
    assert not missing, f"no independent reference for {sorted(missing)}"
    return singles


def _negations(graph, single):
    """What `~atom` selects on edges: the other edges for an edge predicate; for a node
    predicate, the edges between the nodes that fail it."""
    every = _ids(graph.edges)
    node_sets = _node_sets(graph)
    negated = {}
    for name, ids in single.items():
        if name in VIEWS:
            continue
        if name in NODE_KIND:
            outside = _all_names(graph) - node_sets[name]
            negated[name] = frozenset(_both_endpoints(graph, outside))
        else:
            negated[name] = every - ids
    return negated


@with_variants(_init)
def test_single_filters_match_graph_filter_and_chained_views():
    def check(graph):
        atoms, mismatches = _atoms(), []
        view_ref = {
            "layer": graph.layers(["work"]),
            "layers2": graph.layers(["work", "friends"]),
            "before": graph.before(10),
            "after": graph.after(8),
            "window": graph.window(3, 12),
            "at": graph.at(10),
            "latest": graph.latest(),
            "snap_at": graph.snapshot_at(10),
            "snap_latest": graph.snapshot_latest(),
        }
        for name, expr in atoms.items():
            got = _ids(graph.edges[expr])
            if got != _ids(graph.filter(expr).edges):
                mismatches.append(f"{name}: edges[] vs filter()")
            if name in view_ref and got != _ids(view_ref[name].edges):
                mismatches.append(f"{name}: edges[] vs chained view")
        assert not mismatches, mismatches

    return check


@with_variants(_init)
def test_edge_collection_time_view_actually_narrows():
    def check(graph):
        # The failing direction was silent and open: `before` must drop the later edges, not keep
        # the whole collection.
        narrowed = graph.edges[Graph.before(10)]
        assert len(narrowed) < len(graph.edges)
        assert ("c", "a") not in _ids(narrowed)

    return check


@with_variants(_init)
def test_working_combinations_follow_set_algebra():
    def check(graph):
        atoms, single = _atoms(), _singles(graph)
        negated = _negations(graph, single)
        every = _ids(graph.edges)
        cases = []
        for a, b in combinations(atoms, 2):
            if not _and_is_broken(a, b):
                cases.append((f"{a} & {b}", atoms[a] & atoms[b], single[a] & single[b]))
            if _or_is_refused(a, b):
                continue
            if _kind(a) == _kind(b):
                cases.append((f"{a} | {b}", atoms[a] | atoms[b], single[a] | single[b]))
                cases.append(
                    (
                        f"~({a} & {b})",
                        ~(atoms[a] & atoms[b]),
                        negated[a] | negated[b],
                    )
                )
                cases.append(
                    (
                        f"~({a} | {b})",
                        ~(atoms[a] | atoms[b]),
                        negated[a] & negated[b],
                    )
                )
            else:
                cases.append((f"{a} | {b}", atoms[a] | atoms[b], every))
                cases.append(
                    (
                        f"~({a} & {b})",
                        ~(atoms[a] & atoms[b]),
                        negated[a] & negated[b],
                    )
                )
                cases.append((f"~({a} | {b})", ~(atoms[a] | atoms[b]), every))
        for a in atoms:
            if not _not_is_refused(a):
                cases.append((f"~{a}", ~atoms[a], negated[a]))
        cases.append(
            (
                "layer & (edge_prop | src)",
                atoms["layer"] & (atoms["edge_prop"] | atoms["src"]),
                single["layer"] & (single["edge_prop"] | single["src"]),
            )
        )
        mismatches = []
        for label, expr, want in cases:
            for path, got in (
                ("edges[]", _ids(graph.edges[expr])),
                ("filter()", _ids(graph.filter(expr).edges)),
            ):
                if got != want:
                    mismatches.append(
                        f"[{path}] {label}: got {sorted(got)} want {sorted(want)}"
                    )
        assert not mismatches, "\n".join(mismatches)

    return check


@with_variants(_init)
def test_nested_edge_collection_matches_the_graph_filter():
    def check(graph):
        atoms = _atoms()
        working = dict(atoms)
        working["edge_prop & layer"] = atoms["edge_prop"] & atoms["layer"]
        working["edge_prop | src"] = atoms["edge_prop"] | atoms["src"]
        working["~edge_prop"] = ~atoms["edge_prop"]
        for label, expr in working.items():
            indexed = sorted(e.id for es in graph.nodes.edges[expr] for e in es)
            reference = sorted(
                e.id for es in graph.filter(expr).nodes.edges for e in es
            )
            assert indexed == reference, label

    return check


def _subset(atoms):
    """A representative slice of the working shapes: one atom per family plus one composite each."""
    return {
        "edge_prop": atoms["edge_prop"],
        "node_prop": atoms["node_prop"],
        "window": atoms["window"],
        "layer": atoms["layer"],
        "is_deleted": atoms["is_deleted"],
        "edge_prop & layer": atoms["edge_prop"] & atoms["layer"],
        "edge_prop | dst": atoms["edge_prop"] | atoms["dst"],
        "~edge_prop": ~atoms["edge_prop"],
    }


@with_variants(_init)
def test_single_node_edge_collection_selects_incident_edges():
    def check(graph):
        atoms, single = _atoms(), _singles(graph)
        every = _ids(graph.edges)
        want_sets = {
            "edge_prop": single["edge_prop"],
            "node_prop": single["node_prop"],
            "window": single["window"],
            "layer": single["layer"],
            "is_deleted": single["is_deleted"],
            "edge_prop & layer": single["edge_prop"] & single["layer"],
            "edge_prop | dst": single["edge_prop"] | single["dst"],
            "~edge_prop": every - single["edge_prop"],
        }
        exprs = _subset(atoms)
        for name in ("a", "b"):
            node = graph.node(name)
            incident = _ids(node.edges)
            for label, expr in exprs.items():
                got = _ids(node.edges[expr])
                assert got == incident & want_sets[label], f"node {name}: {label}"
                # A node filter the anchor itself fails leaves it with no edges.
                filtered_node = graph.filter(expr).node(name)
                reference = (
                    _ids(filtered_node.edges)
                    if filtered_node is not None
                    else frozenset()
                )
                assert got == reference, f"node {name}: {label} vs graph filter"

    return check


@with_variants(_init)
def test_hop_from_selected_edges_returns_unfiltered_endpoints():
    def check(graph):
        atoms, single = _atoms(), _singles(graph)
        every = _ids(graph.edges)
        want_sets = {
            "edge_prop": single["edge_prop"],
            "node_prop": single["node_prop"],
            "window": single["window"],
            "layer": single["layer"],
            "is_deleted": single["is_deleted"],
            "edge_prop & layer": single["edge_prop"] & single["layer"],
            "edge_prop | dst": single["edge_prop"] | single["dst"],
            "~edge_prop": every - single["edge_prop"],
        }
        for label, expr in _subset(atoms).items():
            selected = graph.edges[expr]
            assert sorted(n.name for n in selected.src) == sorted(
                s for s, _ in want_sets[label]
            ), f"{label}: src"
            assert sorted(n.name for n in selected.dst) == sorted(
                d for _, d in want_sets[label]
            ), f"{label}: dst"
        # `[...]` selects but hands the endpoints back unfiltered: through a window that excludes
        # c's outgoing edge, hopped c still sees its whole neighbourhood.
        for n in graph.edges[Graph.window(3, 12)].dst:
            if n.name == "c":
                assert n.out_degree() == 1

    return check


@with_variants(_init)
def test_a_view_applies_first_under_and():
    """`view & X` means `graph.<view>().filter(X)`: the view is applied first and `X` is
    evaluated inside it, on both the subscript and the `filter()` path. Two views chain.
    """

    def check(graph):
        atoms, views = _atoms(), _view_references(graph)
        mismatches = []
        for v, viewed in views.items():
            for name, atom in atoms.items():
                if name in VIEWS:
                    continue
                want = _ids(viewed.edges[atom])
                for path, got in (
                    ("edges[]", _ids(graph.edges[atoms[v] & atom])),
                    ("filter()", _ids(graph.filter(atoms[v] & atom).edges)),
                ):
                    if got != want:
                        mismatches.append(
                            f"[{path}] {v} & {name}: got {sorted(got)} want {sorted(want)}"
                        )
        # Two views: the second applies inside the first.
        want = _ids(graph.window(3, 12).layers(["work"]).edges)
        got = _ids(graph.filter(atoms["window"] & atoms["layer"]).edges)
        if got != want:
            mismatches.append(
                f"[filter()] window & layer: got {sorted(got)} want {sorted(want)}"
            )
        assert not mismatches, "\n".join(mismatches)

    return check


@with_variants(_init)
def test_a_view_under_or_or_not_is_refused():
    """A view applies to the whole filter, so it composes with `&` only. Under `|` or `~` the
    engine has no meaning to give it, and the expression is refused where it is written.
    """

    def check(graph):
        atoms = _atoms()
        for label, build in {
            "edge_prop | layer": lambda: atoms["edge_prop"] | atoms["layer"],
            "~layer": lambda: ~atoms["layer"],
            "~(edge_prop & layer)": lambda: ~(atoms["edge_prop"] & atoms["layer"]),
            "(edge_prop & layer) | src": lambda: (atoms["edge_prop"] & atoms["layer"])
            | atoms["src"],
        }.items():
            with pytest.raises(TypeError, match="view"):
                build()

    return check
