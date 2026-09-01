"""`.filter()` keeps every member and restricts it to the filtered view; `[...]` selects the
members that pass and returns them unfiltered. The two must be consistent across every filter
field — a filter with an enumerable domain (name/id) used to wrongly drop members from `.filter()`
while a property filter kept them."""

from raphtory import filter
from utils import with_variants

Node = filter.Node


def _init(graph):
    graph.add_node(1, "a", {"industry": "finance"})
    graph.add_node(1, "b", {"industry": "tech"})
    return graph


@with_variants(_init)
def test_filter_keeps_membership_across_fields():
    def check(graph):
        for expr in [
            Node.name() == "a",
            Node.id() == "a",
            Node.property("industry") == "finance",
        ]:
            view = graph.nodes.filter(expr)
            assert sorted(view.name) == ["a", "b"]
            assert len(view) == 2

    return check


@with_variants(_init)
def test_getitem_selects_passing_members():
    def check(graph):
        for expr in [Node.name() == "a", Node.property("industry") == "finance"]:
            view = graph.nodes[expr]
            assert sorted(view.name) == ["a"]
            assert len(view) == 1
        # A graph-level filter also narrows membership.
        assert sorted(graph.filter(Node.name() == "a").nodes.name) == ["a"]

    return check
