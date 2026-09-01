"""`g.edges[expr]` must narrow the same as the graph-level filter `g.filter(expr).edges`, for every
filter type — property, endpoint, layer, time view, and their and/or/not composites. A regression
dropped time views on edge collections, silently returning every edge."""

from raphtory import filter
from utils import with_variants

Graph = filter.Graph
Node = filter.Node
Edge = filter.Edge


def _init(graph):
    graph.add_edge(5, "a", "b", {"weight": 3}, layer="work")
    graph.add_edge(10, "b", "c", {"weight": 8}, layer="work")
    graph.add_edge(15, "c", "a", {"weight": 20})
    return graph


def _exprs():
    weight_gt_5 = Edge.property("weight") > 5
    return {
        "edge_property": weight_gt_5,
        "src": Edge.src().name() == "a",
        "dst": Edge.dst().name() == "c",
        "node_endpoints": Node.name() == "a",
        "layer": Graph.layer("work"),
        "before": Graph.before(10),
        "after": Graph.after(10),
        "window": Graph.window(3, 12),
        "at": Graph.at(10),
        "latest": Graph.latest(),
        "and": weight_gt_5 & Graph.layer("work"),
        "or": (Edge.property("weight") > 15) | (Edge.src().name() == "a"),
        "not": ~weight_gt_5,
        "and_of_or": Graph.layer("work")
        & ((Edge.property("weight") > 15) | (Edge.src().name() == "a")),
    }


def _edges(collection):
    return sorted((e.src.name, e.dst.name) for e in collection)


@with_variants(_init)
def test_edge_collection_index_matches_the_graph_filter_for_every_type():
    def check(graph):
        for label, expr in _exprs().items():
            assert _edges(graph.edges[expr]) == _edges(graph.filter(expr).edges), label

    return check


@with_variants(_init)
def test_edge_collection_time_view_actually_narrows():
    def check(graph):
        # The failing direction was silent and open: `before` must drop the later edges, not keep
        # the whole collection.
        narrowed = graph.edges[Graph.before(10)]
        assert len(narrowed) < len(graph.edges)
        assert ("c", "a") not in _edges(narrowed)

    return check


@with_variants(_init)
def test_nested_edge_collection_index_matches_the_graph_filter():
    def check(graph):
        for label, expr in _exprs().items():
            indexed = _edges(e for es in graph.nodes.edges[expr] for e in es)
            reference = _edges(e for es in graph.filter(expr).nodes.edges for e in es)
            assert indexed == reference, label

    return check
