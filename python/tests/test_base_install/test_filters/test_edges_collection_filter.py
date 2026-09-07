"""Edge-collection filtering across every filter type and combination.

Singles are checked against the graph-level filter and the chained-view
references. Combinations are checked against expectations derived by `_Oracle`
from the single-filter results, under two rules:

* A node expression — a node predicate, or `&`/`|`/`~` of nothing but node
  predicates — is one node test. Applied to edges it selects the edges whose
  endpoints both pass it, so `~N` is "neither endpoint passes N" and `N1 | N2`
  is "both endpoints are N1-or-N2 nodes".
* Everything else is set algebra over edge sets: `&` is intersection, `|` is
  union, `~` is complement, with each maximal node expression contributing the
  edge set above.

`edges[expr]` returns exactly that edge set. `graph.filter(expr)` returns a
graph, so it additionally keeps only edges whose endpoints it kept: nodes are
decided first (an edge predicate says nothing about a node, so it excludes
none), and edges are the edge set induced on those nodes.
"""

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


# `d` is never added as a node; it exists only through its self-loop. A node
# whose every event is an edge event disappears from a filtered graph once those
# edges are filtered out — it has no activity left — exactly as under a single
# edge filter. The oracle models that; the other nodes have events of their own.
EDGE_ONLY_NODES = frozenset({"d"})


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


class _Shape:
    """A filter expression and a symbolic mirror of it, built together, so an
    expectation can be derived from the mirror without evaluating the filter."""

    def __init__(self, label, expr, tree):
        self.label, self.expr, self.tree = label, expr, tree

    def __and__(self, other):
        return _Shape(
            f"({self.label} & {other.label})",
            self.expr & other.expr,
            ("and", self.tree, other.tree),
        )

    def __or__(self, other):
        return _Shape(
            f"({self.label} | {other.label})",
            self.expr | other.expr,
            ("or", self.tree, other.tree),
        )

    def __invert__(self):
        return _Shape(f"~{self.label}", ~self.expr, ("not", self.tree))


def _leaves(tree):
    return {tree[1]} if tree[0] == "atom" else set().union(*map(_leaves, tree[1:]))


def _is_node_expression(tree):
    return _leaves(tree) <= NODE_KIND


def _shapes(atoms):
    """Every pair under `&`, `|`, `~(&)` and `~(|)`, every atom negated, and a
    few deeper nestings, mixing kinds freely."""
    leaf = {name: _Shape(name, expr, ("atom", name)) for name, expr in atoms.items()}
    shapes = []
    for a, b in combinations(leaf, 2):
        shapes += [
            leaf[a] & leaf[b],
            leaf[a] | leaf[b],
            ~(leaf[a] & leaf[b]),
            ~(leaf[a] | leaf[b]),
        ]
    shapes += [~leaf[a] for a in leaf]
    shapes += [
        leaf["layer"] & (leaf["edge_prop"] | leaf["src"]),
        (leaf["node_prop"] | leaf["node_name"]) & leaf["edge_prop"],
        ~(leaf["node_prop"] | leaf["node_name"]) & leaf["window"],
        ~(~leaf["node_name"] & leaf["layer"]),
        leaf["edge_prop"] | ~(leaf["node_prop"] & leaf["node_name"]),
    ]
    return shapes


class _Oracle:
    """Expected results for a shape, from the two rules in the module docstring.

    Fed only with references computed *without* the paths under test (chained
    views for view atoms, direct evaluation for predicates) so an agreement
    means something.
    """

    def __init__(self, graph, single):
        self.single = single
        self.every_edges = _ids(graph.edges)
        self.every_nodes = frozenset(graph.nodes.name)
        self.endpoints = {e.id: (e.src.name, e.dst.name) for e in graph.edges}
        views = _view_references(graph)
        self.node_sets = {
            "node_prop": frozenset(_node_scores(graph, 15)),
            "node_name": frozenset({"b", "c"}) & self.every_nodes,
            **{name: frozenset(view.nodes.name) for name, view in views.items()},
        }

    def _both_endpoints_in(self, nodes):
        return frozenset(
            eid for eid, (s, d) in self.endpoints.items() if s in nodes and d in nodes
        )

    def node_set(self, tree):
        """A node expression as a set of nodes: plain set algebra."""
        kind = tree[0]
        if kind == "atom":
            return self.node_sets[tree[1]]
        if kind == "and":
            return self.node_set(tree[1]) & self.node_set(tree[2])
        if kind == "or":
            return self.node_set(tree[1]) | self.node_set(tree[2])
        return self.every_nodes - self.node_set(tree[1])

    def edge_set(self, tree):
        """What `edges[expr]` selects."""
        if _is_node_expression(tree):
            return self._both_endpoints_in(self.node_set(tree))
        kind = tree[0]
        if kind == "atom":
            return frozenset(self.single[tree[1]])
        if kind == "and":
            return self.edge_set(tree[1]) & self.edge_set(tree[2])
        if kind == "or":
            return self.edge_set(tree[1]) | self.edge_set(tree[2])
        return self.every_edges - self.edge_set(tree[1])

    def _membership(self, tree):
        """Nodes of `graph.filter(expr)` as a (lo, hi) pair of node sets.

        An edge predicate is *unknown* for a node — it neither admits nor
        excludes it — so it spans (nothing, everything); a node predicate or a
        view is definite. `and`/`or` combine bound by bound and `not` swaps
        them. A node is kept unless the expression is definitely false about
        it, which is `hi`.
        """
        kind = tree[0]
        if kind == "atom":
            if _kind(tree[1]) == "edge":
                return frozenset(), self.every_nodes
            nodes = self.node_sets[tree[1]]
            return nodes, nodes
        if kind == "and":
            (l1, h1), (l2, h2) = self._membership(tree[1]), self._membership(tree[2])
            return l1 & l2, h1 & h2
        if kind == "or":
            (l1, h1), (l2, h2) = self._membership(tree[1]), self._membership(tree[2])
            return l1 | l2, h1 | h2
        lo, hi = self._membership(tree[1])
        return self.every_nodes - hi, self.every_nodes - lo

    def graph_edges(self, tree):
        """What `graph.filter(expr).edges` selects: the edge set, induced on the
        nodes the expression admits."""
        return self.edge_set(tree) & self._both_endpoints_in(self._membership(tree)[1])

    def graph_nodes(self, tree):
        """What `graph.filter(expr).nodes` selects: the nodes the expression
        admits, minus any node that has no activity left (see `EDGE_ONLY_NODES`)."""
        admitted = self._membership(tree)[1]
        touched = {name for eid in self.graph_edges(tree) for name in self.endpoints[eid]}
        return frozenset(n for n in admitted if n not in EDGE_ONLY_NODES or n in touched)


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


def _predicate_references(graph):
    return {
        "edge_prop": {
            e.id for e in graph.edges if (e.properties.get("weight") or 0) > 5
        },
        "src": {e.id for e in graph.edges if e.src.name == "a"},
        "dst": {e.id for e in graph.edges if e.dst.name == "c"},
        "node_prop": _both_endpoints(graph, _node_scores(graph, 15)),
        "node_name": _both_endpoints(graph, {"b", "c"}),
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


def _assert_discriminating(graph, single, names):
    """Reject reference sets that cannot tell a right answer from a wrong one.

    The set-algebra expectations below are derived from single-filter results, so
    a single filter that selects everything (or nothing) makes the derived
    expectation degenerate: `EVERYTHING & X == X` is equally consistent with a
    correct `and` and with one that dropped a term. That is not hypothetical —
    on a build where a single view filter fails open, every `view & pred`
    expectation collapses onto the predicate alone, so a broken combination
    matches its expectation and the pins below would report it as fixed.

    Asserting up front that each baseline is a proper subset keeps the pins
    honest wherever this file is run, instead of only on a build where the
    singles happen to be correct.
    """
    every = _ids(graph.edges)
    for name in names:
        assert single[name], f"baseline edges[{name}] selects nothing on this build"
        assert single[name] != every, (
            f"baseline edges[{name}] selects every edge on this build, so any "
            f"expectation derived from it cannot discriminate"
        )


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
def test_composites_follow_the_two_rules_on_both_paths():
    """Every shape in `_shapes`, on `edges[...]` and on `graph.filter(...)`.

    A case whose expectation is the whole collection cannot tell a correct
    answer from one that ignored the filter, so those are skipped per side.
    """

    def check(graph):
        oracle = _Oracle(graph, _singles(graph))
        mismatches = []
        for shape in _shapes(_atoms()):
            want = oracle.edge_set(shape.tree)
            if want != oracle.every_edges:
                got = _ids(graph.edges[shape.expr])
                if got != want:
                    mismatches.append(
                        f"[edges[]] {shape.label}: got {sorted(got)} want {sorted(want)}"
                    )
            filtered = graph.filter(shape.expr)
            want = oracle.graph_edges(shape.tree)
            if want != oracle.every_edges:
                got = _ids(filtered.edges)
                if got != want:
                    mismatches.append(
                        f"[filter().edges] {shape.label}: got {sorted(got)} want {sorted(want)}"
                    )
            want = oracle.graph_nodes(shape.tree)
            if want != oracle.every_nodes:
                got = frozenset(filtered.nodes.name)
                if got != want:
                    mismatches.append(
                        f"[filter().nodes] {shape.label}: got {sorted(got)} want {sorted(want)}"
                    )
        assert not mismatches, "\n".join(mismatches)

    return check


@with_variants(_init)
def test_graph_filter_is_an_induced_subgraph():
    """A filtered graph never holds an edge whose endpoint it dropped."""

    def check(graph):
        dangling = []
        for shape in _shapes(_atoms()):
            filtered = graph.filter(shape.expr)
            nodes = frozenset(filtered.nodes.name)
            for e in filtered.edges:
                if e.src.name not in nodes or e.dst.name not in nodes:
                    dangling.append(f"{shape.label}: {e.id}")
        assert not dangling, dangling

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
            reference = sorted(
                e.id for es in graph.filter(expr).nodes.edges for e in es
            )
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
def test_negating_a_node_filter_selects_edges_among_the_remaining_nodes():
    """`~N` is one node test — the nodes that fail N — and an edge passes a node
    test when both its endpoints do. So `edges[~N]` is the edges among the
    failing nodes, not the complement of `edges[N]`, which would keep every edge
    that merely touches one failing node. Both spellings agree.
    """

    def check(graph):
        atoms, oracle = _atoms(), _Oracle(graph, _singles(graph))
        for name in NODE_KIND:
            remaining = oracle.every_nodes - oracle.node_sets[name]
            want = oracle.edge_set(("not", ("atom", name)))
            assert want == oracle._both_endpoints_in(remaining)
            # The two readings differ on this graph, so the assertions below
            # discriminate.
            assert want != oracle.every_edges - oracle.single[name]
            assert _ids(graph.edges[~atoms[name]]) == want, f"edges[~{name}]"
            assert _ids(graph.filter(~atoms[name]).edges) == want, f"filter(~{name})"

    return check


@with_variants(_init)
def test_node_expression_does_not_test_the_node_stood_on():
    """Walking from a node through a node-filtered graph tests the neighbours,
    not the node walked from — for a composite exactly as for a single filter.
    `a` fails both predicates here; its in-neighbour `c` passes both."""

    def check(graph):
        atoms = _atoms()
        single = frozenset(graph.node("a").filter(atoms["node_prop"]).in_neighbours.name)
        assert single == {"c"}
        for label, expr in (
            ("&", atoms["node_prop"] & atoms["node_name"]),
            ("|", atoms["node_prop"] | atoms["node_name"]),
            ("~~", ~~atoms["node_prop"]),
        ):
            got = frozenset(graph.node("a").filter(expr).in_neighbours.name)
            assert got == single, f"{label}: {sorted(got)}"

    return check


@with_variants(_init)
def test_edge_predicates_say_nothing_about_nodes():
    """In `graph.filter(expr).nodes`, an edge predicate neither admits nor
    excludes a node: a node goes only when the expression is definitely false
    about it, or when it has no activity left. That keeps de Morgan: `~(P & N)`
    and `~P | ~N` agree."""

    def check(graph):
        atoms, oracle = _atoms(), _Oracle(graph, _singles(graph))
        every, high = oracle.every_nodes, oracle.node_sets["node_prop"]
        P, N = atoms["edge_prop"], atoms["node_prop"]
        assert high and high != every
        # `d` has no activity of its own and `P | N` drops its only edge, so it
        # goes — as it does under `P` alone, which is the point of comparison.
        under_p_alone = frozenset(graph.filter(P).nodes.name)
        assert under_p_alone == every - EDGE_ONLY_NODES
        cases = {
            "P & N": (P & N, high),
            "P | N": (P | N, under_p_alone),
            "~P": (~P, every),
            "~(P & N)": (~(P & N), every),
            "~P | ~N": (~P | ~N, every),
            "~(P | N)": (~(P | N), every - high),
            "~P & ~N": (~P & ~N, every - high),
        }
        for label, (expr, want) in cases.items():
            got = frozenset(graph.filter(expr).nodes.name)
            assert got == want, f"{label}: got {sorted(got)} want {sorted(want)}"

    return check


@with_variants(_init)
def test_per_node_edge_collections_with_a_node_filter_are_still_broken():
    """`nodes.edges[node-filter]` and `node(x).edges[node-filter]` still fail
    open when the anchor fails the predicate. When that is fixed this fails:
    drop the `NODE_KIND` exclusions in the nested and single-node tests above.
    """

    def check(graph):
        atoms = _atoms()
        nested = sorted(
            e.id for es in graph.nodes.edges[atoms["node_prop"]] for e in es
        )
        nested_ref = sorted(
            e.id for es in graph.filter(atoms["node_prop"]).nodes.edges for e in es
        )
        per_node = _ids(graph.node("a").edges[atoms["node_prop"]])
        assert not (
            nested == nested_ref and per_node == frozenset()
        ), "now FIXED: per-node/nested edges with a node filter — drop the NODE_KIND exclusions"

    return check
