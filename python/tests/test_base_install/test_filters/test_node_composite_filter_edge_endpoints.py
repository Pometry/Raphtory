"""Regression tests: an `and` of node filters must not keep edges to excluded nodes.

Existing composite-filter tests only assert on `nodes.id`, so they never noticed that
`graph.filter(A & B)` could return an edge whose endpoint is not in the (correctly intersected) node
set. The structural invariant checked here is: for any view, every edge endpoint is a node of the
view (`view.has_node(endpoint)`).

The underlying bug was in `edges_inner` (raphtory/src/db/api/view/graph.rs): it walked an untrusted
node-list superset and trusted each edge's source, leaking edges to filtered-out nodes.
"""

from itertools import combinations

from raphtory import filter
from utils import with_variants

Node = filter.Node


def _init(graph):
    # Nodes 0..5 with a name, a type, and a numeric property; edges that cross filter boundaries so
    # a filtered-out endpoint can leave a dangling edge.
    for i in range(6):
        graph.add_node(0, i, {"val": i}, "A" if i % 2 == 0 else "B")
    for src, dst in [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 0),
        (0, 3),
        (1, 4),
        (2, 5),
        (5, 2),
        (4, 0),
    ]:
        graph.add_edge(0, src, dst, {"w": src + dst})
    return graph


# Node-filter legs spanning fields (id / name / type / property / degree) and operators.
LEGS = {
    "id.is_in([0,2,4])": Node.id().is_in([0, 2, 4]),
    "id.is_not_in([5])": Node.id().is_not_in([5]),
    "name.contains('3')": Node.name().contains("3"),
    "name.is_in(['2','3','4'])": Node.name().is_in(["2", "3", "4"]),
    "type.is_in(['A'])": Node.node_type().is_in(["A"]),
    "prop val>2": Node.property("val") > 2,
    "prop val<4": Node.property("val") < 4,
    "prop val.is_in([1,3,5])": Node.property("val").is_in([1, 3, 5]),
    "degree>1": Node.degree() > 1,
}


def _dangling_edges(view):
    """Edges whose src or dst is not a node of the view (should always be empty)."""
    return [
        (e.src.id, e.dst.id)
        for e in view.edges
        if not view.has_node(e.src.id) or not view.has_node(e.dst.id)
    ]


@with_variants(_init)
def test_and_filter_keeps_edge_endpoints_minimal():
    def check(graph):
        # Keep node ids {0,2,4} AND val < 4  ->  nodes {0,2}. Edge 4->0 must not survive (node 4 gone).
        view = graph.filter(Node.id().is_in([0, 2, 4]) & (Node.property("val") < 4))
        assert sorted(view.nodes.id) == [0, 2]
        assert (
            _dangling_edges(view) == []
        ), "edge to an excluded node survived the filter"

    return check


@with_variants(_init)
def test_and_filter_combinations_keep_edge_endpoints():
    def check(graph):
        legs = list(LEGS.items())
        combos = list(combinations(legs, 2)) + list(combinations(legs, 3))
        offenders = {}
        for combo in combos:
            expr = combo[0][1]
            for _, leg in combo[1:]:
                expr = expr & leg
            dangling = _dangling_edges(graph.filter(expr))
            if dangling:
                offenders[" & ".join(label for label, _ in combo)] = dangling
        assert not offenders, (
            f"{len(offenders)}/{len(combos)} node-filter combinations leaked edges to excluded "
            f"nodes, e.g. "
            + "; ".join(f"{k} -> {v}" for k, v in list(offenders.items())[:3])
        )

    return check
