"""Edge-collection filtering across every filter type and combination.

Singles are checked against the graph-level filter and the chained-view references. Combinations
are checked against set algebra over the single-filter results (`&` = intersection, `|` = union,
`~` = complement), which is how node collections already behave. The combination classes that are
known broken on edges are pinned by `test_broken_combination_classes_are_still_broken` — when a fix
lands, that test fails and the class moves into the working set by deleting its rule below."""

from itertools import combinations

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
    # A time view combined with anything via `and` is silently ignored.
    return a in TIME_VIEWS or b in TIME_VIEWS


def _or_is_broken(a, b):
    # An `or` involving any graph view, or mixing edge- and node-kind operands, returns every edge.
    return a in VIEWS or b in VIEWS or _kind(a) != _kind(b)


def _not_is_broken(a):
    # `~view` returns every edge; `~node-filter` distributes the negation into the endpoints
    # instead of complementing the matching edge set.
    return a in VIEWS or a in NODE_KIND


def _ids(collection):
    return frozenset(e.id for e in collection)


def _singles(graph):
    return {name: _ids(graph.edges[expr]) for name, expr in _atoms().items()}


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
        every = _ids(graph.edges)
        cases = []
        for a, b in combinations(atoms, 2):
            if not _and_is_broken(a, b):
                cases.append((f"{a} & {b}", atoms[a] & atoms[b], single[a] & single[b]))
            if not _or_is_broken(a, b):
                cases.append((f"{a} | {b}", atoms[a] | atoms[b], single[a] | single[b]))
        for a in atoms:
            if not _not_is_broken(a):
                cases.append((f"~{a}", ~atoms[a], every - single[a]))
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
                    mismatches.append(f"[{path}] {label}: got {sorted(got)} want {sorted(want)}")
        assert not mismatches, "\n".join(mismatches)

    return check


@with_variants(_init)
def test_nested_edge_collection_matches_the_graph_filter():
    def check(graph):
        atoms = _atoms()
        # Node-kind filters fail open on the nested path — pinned in the broken-classes test.
        working = {n: e for n, e in atoms.items() if n not in NODE_KIND}
        working["edge_prop & layer"] = atoms["edge_prop"] & atoms["layer"]
        working["edge_prop | src"] = atoms["edge_prop"] | atoms["src"]
        working["~edge_prop"] = ~atoms["edge_prop"]
        for label, expr in working.items():
            indexed = sorted(e.id for es in graph.nodes.edges[expr] for e in es)
            reference = sorted(e.id for es in graph.filter(expr).nodes.edges for e in es)
            assert indexed == reference, label

    return check


def _subset(atoms):
    """A representative slice of the working shapes: one atom per family plus one composite each."""
    return {
        "edge_prop": atoms["edge_prop"],
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
            "window": single["window"],
            "layer": single["layer"],
            "is_deleted": single["is_deleted"],
            "edge_prop & layer": single["edge_prop"] & single["layer"],
            "edge_prop | dst": single["edge_prop"] | single["dst"],
            "~edge_prop": every - single["edge_prop"],
        }
        # Node-kind filters fail open here too when the anchor node fails the predicate — pinned
        # in the broken-classes test.
        exprs = _subset(atoms)
        for name in ("a", "b"):
            node = graph.node(name)
            incident = _ids(node.edges)
            for label, expr in exprs.items():
                got = _ids(node.edges[expr])
                assert got == incident & want_sets[label], f"node {name}: {label}"
                reference = _ids(graph.filter(expr).node(name).edges)
                assert got == reference, f"node {name}: {label} vs graph filter"

    return check


@with_variants(_init)
def test_hop_from_selected_edges_returns_unfiltered_endpoints():
    def check(graph):
        atoms, single = _atoms(), _singles(graph)
        every = _ids(graph.edges)
        want_sets = {
            "edge_prop": single["edge_prop"],
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
def test_broken_combination_classes_are_still_broken():
    """One discriminating representative per known-broken class. When a class is fixed this fails:
    delete its `_*_is_broken` rule above so the combinations join the set-algebra test."""

    def check(graph):
        atoms, single = _atoms(), _singles(graph)
        every = _ids(graph.edges)
        representatives = {
            "and drops a time view": (
                atoms["window"] & atoms["edge_prop"],
                single["window"] & single["edge_prop"],
            ),
            "or with a view returns every edge": (
                atoms["edge_prop"] | atoms["layer"],
                single["edge_prop"] | single["layer"],
            ),
            "or of mixed kinds returns every edge": (
                atoms["edge_prop"] | atoms["node_prop"],
                single["edge_prop"] | single["node_prop"],
            ),
            "not of a view returns every edge": (~atoms["layer"], every - single["layer"]),
            "not of a node filter is not the complement": (
                ~atoms["node_name"],
                every - single["node_name"],
            ),
        }
        fixed = []
        for label, (expr, want) in representatives.items():
            if _ids(graph.edges[expr]) == want and _ids(graph.filter(expr).edges) == want:
                fixed.append(label)
        nested = sorted(e.id for es in graph.nodes.edges[atoms["node_prop"]] for e in es)
        nested_ref = sorted(e.id for es in graph.filter(atoms["node_prop"]).nodes.edges for e in es)
        per_node = _ids(graph.node("a").edges[atoms["node_prop"]])
        if nested == nested_ref and per_node == frozenset():
            fixed.append("per-node/nested edges with a node filter")
        assert not fixed, (
            f"now FIXED: {fixed} — move the class into the working set by deleting its rule"
        )

    return check
