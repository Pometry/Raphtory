"""Filter algebra across every shape and every collection path.

Filtering is spelled several ways — `nodes[expr]`, `edges[expr]`,
`graph.filter(expr)` — and the spellings lower an expression differently. They
agree on single predicates and diverge on composites, so the interesting surface
is the cross-product {atom kind} x {`&`, `|`, `~`, `~(A & B)`, `~(A | B)`} x
{path}, checked against set algebra.

Two rules keep the expectations trustworthy, both learned from filter bugs that
the existing tests could not see:

**The oracle never comes from the path under test.** Each atom carries a
reference set built another way: a view atom from the equivalent chained view
(`graph.window(a, b).edges`), a predicate atom by evaluating it in Python over
the collection. Composite expectations are set algebra over those references.
Deriving them from `edges[atom]` instead would let the suite agree with itself —
where a single filter fails open, `EVERYTHING & X == X` is equally consistent
with a correct `and` and with one that dropped a term.

**A case that cannot discriminate is not a case.** Where the correct answer is
the whole universe, an implementation that ignores the filter entirely still
matches, so those combinations are skipped rather than counted. Real bugs have
hidden behind exactly that coincidence.

`nodes[expr]` is asserted positively for every combination: it lowers composites
to per-node boolean operations, so set algebra holds there by construction, and
it is the behaviour the other paths should eventually match.

The other paths compose wrapper graphs instead, which loses a view operand's
restriction, and they are currently wrong for many composites (see
Pometry/pometry-storage#371). Rather than a pin per case, the disagreeing
*groups* are recorded in `KNOWN_DISAGREEMENTS` and compared as a set, so this
module lands green, states the boundary exactly, and fails with the specific
groups that moved when it changes in either direction.
"""

from collections import defaultdict
from itertools import combinations

import pytest
from raphtory import Graph, PersistentGraph, filter

Edge = filter.Edge
Node = filter.Node
GraphFilter = filter.Graph

GRAPH_TYPES = [Graph, PersistentGraph]

# What an atom tests. It decides where the atom may be used (a node collection
# rejects edge predicates outright), how a node filter reduces onto edges, and —
# for time views specifically — which composites go wrong.
EDGE, NODE, LAYER_VIEW, TIME_VIEW = "edge", "node", "layer_view", "time_view"


# --- fixture ----------------------------------------------------------------
#
# Shaped so that no reference set below is empty or the whole universe: weights
# straddle both thresholds, layers and times split the edges, and node `f` has no
# edges at all so node-side and edge-side sets are genuinely different.


def _build(cls):
    graph = cls()
    graph.add_node(1, "f", properties={"score": 99})
    graph.add_edge(5, "a", "b", properties={"weight": 3}, layer="work")
    graph.add_edge(10, "b", "c", properties={"weight": 8}, layer="social")
    graph.add_edge(15, "c", "a", properties={"weight": 20}, layer="social")
    graph.add_edge(20, "d", "d", properties={"weight": 1}, layer="work")
    graph.add_edge(25, "a", "d", properties={"weight": 9}, layer="other")
    for time, name, score in (
        (5, "a", 10),
        (10, "b", 20),
        (15, "c", 30),
        (20, "d", 40),
    ):
        graph.add_node(time, name, properties={"score": score})
    return graph


def _edge_ids(collection):
    return frozenset(edge.id for edge in collection)


def _node_names(collection):
    return frozenset(node.name for node in collection)


# --- atoms ------------------------------------------------------------------


class Atom:
    """A filter atom plus the sets it selects, computed independently."""

    def __init__(self, kind, expr, edges, nodes):
        self.kind = kind
        self.expr = expr
        self._edges = edges
        self._nodes = nodes

    def edge_set(self, graph):
        if self.kind == NODE:
            # A node filter keeps the edges whose *both* endpoints pass.
            names = self._nodes(graph)
            return frozenset(
                edge.id
                for edge in graph.edges
                if edge.src.name in names and edge.dst.name in names
            )
        return self._edges(graph)

    def node_set(self, graph):
        return self._nodes(graph)


def _property_over(reader, key, threshold):
    def compute(graph):
        return frozenset(
            item.id if hasattr(item, "src") else item.name
            for item in reader(graph)
            if (item.properties.get(key) or 0) > threshold
        )

    return compute


def _atoms():
    return {
        "weight_gt_5": Atom(
            EDGE,
            Edge.property("weight") > 5,
            _property_over(lambda g: g.edges, "weight", 5),
            None,
        ),
        "weight_gt_8": Atom(
            EDGE,
            Edge.property("weight") > 8,
            _property_over(lambda g: g.edges, "weight", 8),
            None,
        ),
        "score_gt_15": Atom(
            NODE,
            Node.property("score") > 15,
            None,
            _property_over(lambda g: g.nodes, "score", 15),
        ),
        "name_in_bc": Atom(
            NODE,
            Node.name().is_in(["b", "c"]),
            None,
            lambda graph: frozenset({"b", "c"}),
        ),
        "layer": Atom(
            LAYER_VIEW,
            GraphFilter.layer("social"),
            lambda graph: _edge_ids(graph.layer("social").edges),
            lambda graph: _node_names(graph.layer("social").nodes),
        ),
        "window": Atom(
            TIME_VIEW,
            GraphFilter.window(3, 12),
            lambda graph: _edge_ids(graph.window(3, 12).edges),
            lambda graph: _node_names(graph.window(3, 12).nodes),
        ),
        "before": Atom(
            TIME_VIEW,
            GraphFilter.before(12),
            lambda graph: _edge_ids(graph.before(12).edges),
            lambda graph: _node_names(graph.before(12).nodes),
        ),
    }


# --- shapes and paths -------------------------------------------------------

SHAPES = {
    "and": (2, lambda a, b: a & b, lambda x, y, universe: x & y),
    "or": (2, lambda a, b: a | b, lambda x, y, universe: x | y),
    "not": (1, lambda a, b: ~a, lambda x, y, universe: universe - x),
    # Negation *over a composite*: a pairwise audit that only negates single
    # atoms cannot reach these, and they fail differently from `~atom`.
    "not_and": (2, lambda a, b: ~(a & b), lambda x, y, universe: universe - (x & y)),
    "not_or": (2, lambda a, b: ~(a | b), lambda x, y, universe: universe - (x | y)),
}

# select: how to apply an expression, project: what the result is compared as,
# node_only: whether the path rejects edge-kind atoms.
PATHS = {
    "edges[expr]": (lambda graph, expr: _edge_ids(graph.edges[expr]), EDGE, False),
    "filter(expr).edges": (
        lambda graph, expr: _edge_ids(graph.filter(expr).edges),
        EDGE,
        False,
    ),
    "nodes[expr]": (lambda graph, expr: _node_names(graph.nodes[expr]), NODE, True),
    "filter(expr).nodes": (
        lambda graph, expr: _node_names(graph.filter(expr).nodes),
        NODE,
        True,
    ),
}


# --- the recorded boundary ---------------------------------------------------
#
# (path, shape, sorted atom kinds) for every group that currently disagrees with
# set algebra. Each maps to a mechanism described in #371:
#
#   `and` with a time_view                  the view operand's restriction is
#                                           inherited from the unviewed base and
#                                           evaporates (class 1). `and` with a
#                                           layer_view is *not* here: it works.
#   `or` with any view, or mixed kinds      each side imposes no restriction of
#                                           its own, so the union is everything
#                                           (class 2)
#   `not` with a view                       (class 3)
#   `not` with a node filter                negation distributes into the
#                                           endpoints instead of complementing
#                                           the edge set (class 4)
#   `not_and` / `not_or`                    the same wrappers reached through a
#                                           negated composite; `~(A & view)`
#                                           degenerates to `~A`
#
# `nodes[expr]` appears nowhere: it is correct for every combination.

KNOWN_DISAGREEMENTS = {
    ("edges[expr]", "and", "edge+time_view"),
    ("edges[expr]", "and", "layer_view+time_view"),
    ("edges[expr]", "and", "node+time_view"),
    ("edges[expr]", "and", "time_view"),
    ("edges[expr]", "not", "layer_view"),
    ("edges[expr]", "not", "node"),
    ("edges[expr]", "not", "time_view"),
    ("edges[expr]", "not_and", "edge+layer_view"),
    ("edges[expr]", "not_and", "edge+node"),
    ("edges[expr]", "not_and", "edge+time_view"),
    ("edges[expr]", "not_and", "layer_view+node"),
    ("edges[expr]", "not_and", "layer_view+time_view"),
    ("edges[expr]", "not_and", "node"),
    ("edges[expr]", "not_and", "node+time_view"),
    ("edges[expr]", "not_and", "time_view"),
    ("edges[expr]", "not_or", "edge+layer_view"),
    ("edges[expr]", "not_or", "edge+node"),
    ("edges[expr]", "not_or", "edge+time_view"),
    ("edges[expr]", "not_or", "layer_view+node"),
    ("edges[expr]", "not_or", "layer_view+time_view"),
    ("edges[expr]", "not_or", "node"),
    ("edges[expr]", "not_or", "node+time_view"),
    ("edges[expr]", "not_or", "time_view"),
    ("edges[expr]", "or", "edge+layer_view"),
    ("edges[expr]", "or", "edge+node"),
    ("edges[expr]", "or", "edge+time_view"),
    ("edges[expr]", "or", "layer_view+node"),
    ("edges[expr]", "or", "layer_view+time_view"),
    ("edges[expr]", "or", "node+time_view"),
    ("edges[expr]", "or", "time_view"),
    ("filter(expr).edges", "and", "edge+time_view"),
    ("filter(expr).edges", "and", "layer_view+time_view"),
    ("filter(expr).edges", "and", "node+time_view"),
    ("filter(expr).edges", "and", "time_view"),
    ("filter(expr).edges", "not", "layer_view"),
    ("filter(expr).edges", "not", "node"),
    ("filter(expr).edges", "not", "time_view"),
    ("filter(expr).edges", "not_and", "edge+layer_view"),
    ("filter(expr).edges", "not_and", "edge+node"),
    ("filter(expr).edges", "not_and", "edge+time_view"),
    ("filter(expr).edges", "not_and", "layer_view+node"),
    ("filter(expr).edges", "not_and", "layer_view+time_view"),
    ("filter(expr).edges", "not_and", "node"),
    ("filter(expr).edges", "not_and", "node+time_view"),
    ("filter(expr).edges", "not_and", "time_view"),
    ("filter(expr).edges", "not_or", "edge+layer_view"),
    ("filter(expr).edges", "not_or", "edge+node"),
    ("filter(expr).edges", "not_or", "edge+time_view"),
    ("filter(expr).edges", "not_or", "layer_view+node"),
    ("filter(expr).edges", "not_or", "layer_view+time_view"),
    ("filter(expr).edges", "not_or", "node"),
    ("filter(expr).edges", "not_or", "node+time_view"),
    ("filter(expr).edges", "not_or", "time_view"),
    ("filter(expr).edges", "or", "edge+layer_view"),
    ("filter(expr).edges", "or", "edge+node"),
    ("filter(expr).edges", "or", "edge+time_view"),
    ("filter(expr).edges", "or", "layer_view+node"),
    ("filter(expr).edges", "or", "layer_view+time_view"),
    ("filter(expr).edges", "or", "node+time_view"),
    ("filter(expr).edges", "or", "time_view"),
    ("filter(expr).nodes", "and", "node+time_view"),
    ("filter(expr).nodes", "and", "time_view"),
    ("filter(expr).nodes", "not", "time_view"),
    ("filter(expr).nodes", "not_and", "node+time_view"),
    ("filter(expr).nodes", "not_and", "time_view"),
    ("filter(expr).nodes", "not_or", "node+time_view"),
    ("filter(expr).nodes", "not_or", "time_view"),
    ("filter(expr).nodes", "or", "node+time_view"),
    ("filter(expr).nodes", "or", "time_view"),
}


# --- evaluation -------------------------------------------------------------


def _discriminating(projection):
    """Atom names whose reference set is a proper, non-empty subset.

    An atom whose reference is empty or the whole universe cannot tell a right
    answer from a wrong one on that side, so it is left out of the cases rather
    than silently weakening them. `test_fixture_keeps_every_atom_useful` records
    which exclusions are expected and why.
    """
    graph = _build(Graph)
    universe = _universe(graph, projection)
    useful = []
    for name, atom in _atoms().items():
        if projection == NODE and atom.kind == EDGE:
            continue
        reference = _reference(atom, graph, projection)
        if reference and reference != universe:
            useful.append(name)
    return useful


def _cases(path):
    """(shape, atom names) for every combination this path accepts."""
    _, projection, _ = PATHS[path]
    names = _discriminating(projection)
    for shape, (arity, _, _) in SHAPES.items():
        if arity == 1:
            yield from ((shape, (name,)) for name in names)
        else:
            yield from ((shape, pair) for pair in combinations(names, 2))


def _reference(atom, graph, projection):
    return atom.edge_set(graph) if projection == EDGE else atom.node_set(graph)


def _universe(graph, projection):
    return _edge_ids(graph.edges) if projection == EDGE else _node_names(graph.nodes)


def _evaluate(graph, path, shape, names):
    """(expected, actual, kinds) or None when the case cannot discriminate."""
    select, projection, _ = PATHS[path]
    arity, build, combine = SHAPES[shape]
    atoms = _atoms()
    left = atoms[names[0]]
    right = atoms[names[1]] if arity == 2 else None

    universe = _universe(graph, projection)
    left_set = _reference(left, graph, projection)
    right_set = _reference(right, graph, projection) if right else None
    # Cases are generated from `_discriminating`, so this only catches a case
    # list that has drifted out of step with the fixture.
    for name, reference in zip(names, (left_set, right_set)):
        assert reference and reference != universe, (
            f"reference set for {name} cannot discriminate on the {projection} "
            f"side; it should have been excluded from the cases"
        )

    expected = combine(left_set, right_set, universe)
    if expected == universe:
        # An implementation that ignores the filter would also return this.
        return None

    actual = select(graph, build(left.expr, right.expr if right else None))
    kinds = "+".join(sorted({left.kind} | ({right.kind} if right else set())))
    return expected, actual, kinds


NODE_SUBSCRIPT_CASES = list(_cases("nodes[expr]"))


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
@pytest.mark.parametrize(
    "shape,names",
    NODE_SUBSCRIPT_CASES,
    ids=lambda v: v if isinstance(v, str) else "+".join(v),
)
def test_node_subscript_obeys_set_algebra(cls, shape, names):
    """`nodes[expr]` is correct for every shape, and must stay that way.

    This is the positive half of the module: the one path that reduces
    composition to boolean algebra over membership, so `&` is intersection, `|`
    is union and `~` is complement for every kind of operand.
    """
    graph = _build(cls)
    outcome = _evaluate(graph, "nodes[expr]", shape, names)
    if outcome is None:
        pytest.skip("expectation is the whole universe; cannot discriminate")
    expected, actual, _ = outcome
    assert actual == expected, (
        f"nodes[{shape} of {'+'.join(names)}] selected {sorted(actual)}, "
        f"set algebra says {sorted(expected)}"
    )


def _observed_disagreements(graph_types):
    observed = defaultdict(set)
    for cls in graph_types:
        graph = _build(cls)
        for path in PATHS:
            for shape, names in _cases(path):
                outcome = _evaluate(graph, path, shape, names)
                if outcome is None:
                    continue
                expected, actual, kinds = outcome
                if actual != expected:
                    observed[(path, shape, kinds)].add(cls.__name__)
    return observed


def test_disagreement_ledger_is_unchanged():
    """The set of groups that disagree with set algebra is exactly as recorded.

    Fails in both directions on purpose. A group that starts agreeing means a
    fix landed and its entry should be deleted — the whole point, since the
    lowering these paths share is being replaced by a per-edge boolean one. A
    group that starts disagreeing is a regression, or a shape nobody had tried.
    """
    observed = set(_observed_disagreements(GRAPH_TYPES))
    fixed = KNOWN_DISAGREEMENTS - observed
    regressed = observed - KNOWN_DISAGREEMENTS
    assert not (fixed or regressed), "\n".join(
        [
            "the filter-algebra boundary moved:",
            *(
                f"  NOW CORRECT (delete from KNOWN_DISAGREEMENTS): {group}"
                for group in sorted(fixed)
            ),
            *(
                f"  NOW WRONG (a regression, or a newly covered shape): {group}"
                for group in sorted(regressed)
            ),
        ]
    )


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
def test_edge_spellings_agree_with_each_other(cls):
    """`edges[expr]` and `filter(expr).edges` share one lowering, so they agree
    for every shape — including the shapes both get wrong.

    Which is why agreement between them proves nothing about correctness on its
    own, and why every other test here compares against an independent
    reference instead.
    """
    graph = _build(cls)
    atoms = _atoms()
    mismatches = []
    for shape, names in _cases("edges[expr]"):
        arity, build, _ = SHAPES[shape]
        left = atoms[names[0]]
        right = atoms[names[1]] if arity == 2 else None
        expr = build(left.expr, right.expr if right else None)
        if _edge_ids(graph.edges[expr]) != _edge_ids(graph.filter(expr).edges):
            mismatches.append(f"{shape}-{'+'.join(names)}")
    assert not mismatches, f"the two edge spellings diverge for: {mismatches}"


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
def test_single_views_narrow_on_every_path(cls):
    """A view used as a filter narrows, on every path.

    Compared against the same view spelled as a chain rather than across paths:
    a shape that fails open everywhere agrees with itself, which is how
    `edges[before(t)]` returning every edge went unnoticed.
    """
    graph = _build(cls)
    for name, atom in _atoms().items():
        if atom.kind not in (TIME_VIEW, LAYER_VIEW):
            continue
        edge_reference = atom.edge_set(graph)
        assert edge_reference and edge_reference != _edge_ids(graph.edges), (
            f"view {name} does not narrow the fixture's edges; the assertions "
            f"below would not discriminate"
        )
        assert _edge_ids(graph.edges[atom.expr]) == edge_reference, f"edges[{name}]"
        assert (
            _edge_ids(graph.filter(atom.expr).edges) == edge_reference
        ), f"filter({name}).edges"
        assert _node_names(graph.nodes[atom.expr]) == atom.node_set(
            graph
        ), f"nodes[{name}]"


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
def test_double_negation_is_the_identity_for_predicates(cls):
    """`~~A == A` for predicate atoms.

    View atoms are excluded: `~view` already returns every edge on the edge
    paths, so `~~view` is meaningless there until that is fixed. On
    `nodes[expr]`, where `~` is a real complement, views are included.
    """
    graph = _build(cls)
    for name, atom in _atoms().items():
        if atom.kind in (EDGE, NODE):
            assert _edge_ids(graph.edges[~~atom.expr]) == _edge_ids(
                graph.edges[atom.expr]
            ), f"edges[~~{name}]"
        if atom.kind != EDGE:
            assert _node_names(graph.nodes[~~atom.expr]) == _node_names(
                graph.nodes[atom.expr]
            ), f"nodes[~~{name}]"


def test_fixture_keeps_every_atom_useful():
    """Which atoms can discriminate on which side, and why any cannot.

    `layer` is absent from the node side because a layer view filters edges, not
    nodes: every node survives it, including isolated ones, so as a node-side
    reference it is the whole universe. That is a property of layer views rather
    than of this fixture, so the exclusion is permanent. Anything else appearing
    here means the fixture stopped exercising an atom and the cases built from
    it went quiet.
    """
    assert set(_discriminating(EDGE)) == set(
        _atoms()
    ), "every atom should discriminate on the edge side"
    node_side = set(_discriminating(NODE))
    expected = {name for name, atom in _atoms().items() if atom.kind != EDGE} - {
        "layer"
    }
    assert (
        node_side == expected
    ), f"node-side atoms changed: got {sorted(node_side)}, expected {sorted(expected)}"


@pytest.mark.parametrize("cls", GRAPH_TYPES, ids=lambda c: c.__name__)
def test_node_collections_reject_edge_predicates(cls):
    """A node collection cannot be narrowed by an edge predicate, and says so
    rather than quietly selecting everything."""
    graph = _build(cls)
    with pytest.raises(Exception, match="Node filter expected"):
        _node_names(graph.nodes[Edge.property("weight") > 5])
