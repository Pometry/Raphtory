"""`.filter()` keeps every member and restricts it to the filtered view; `[...]` selects the
members that pass and returns them unfiltered. The two must be consistent across every filter
field — a filter with an enumerable domain (name/id) used to wrongly drop members from `.filter()`
while a property filter kept them."""

from itertools import combinations

import pytest

from raphtory import filter
from utils import with_variants

Node = filter.Node
Graph = filter.Graph


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


def _rich(graph):
    graph.add_node(5, "a", {"score": 10})
    graph.add_node(10, "b", {"score": 20})
    graph.add_node(15, "c", {"score": 30})
    graph.add_edge(5, "a", "b", layer="work")
    graph.add_edge(10, "b", "c", layer="work")
    graph.add_edge(15, "c", "a", layer="friends")
    graph.add_node(20, "d", {"score": 5})
    return graph


@with_variants(_rich)
def test_node_collection_combinations_follow_set_algebra():
    """A representative subset of filter families combined with `&`/`|`/`~`.

    Every expectation is built from chained views and predicates evaluated
    directly, never from the subscript under test. Two of them are not plain
    set algebra over independently evaluated results, deliberately:

    * `&` applies its operands in sequence, so a predicate beside a view is
      read *inside* that view. `prop & window` asks which nodes score above
      the threshold within the window, not which nodes do so at any time and
      also appear in the window.
    * `~view` keeps the events outside the view rather than the entities with
      no event inside it, so the complement of a window is the ranges either
      side of it and the complement of a layer is the other layers.
    """

    def check(graph):
        names = lambda view: frozenset(view.nodes.name)

        # Each atom as an expression, plus the two independent ways of reading
        # it: a view as the equivalent chained view, a predicate as a direct
        # scan of whatever graph it is evaluated against.
        views = {
            "window": lambda g: g.window(3, 12),
            "before": lambda g: g.before(12),
            "layer": lambda g: g.layer("work"),
        }
        preds = {
            "name": lambda g: frozenset({"a", "b"}) & names(g),
            "prop": lambda g: frozenset(
                n.name for n in g.nodes if (n.properties.get("score") or 0) > 15
            ),
        }
        atoms = {
            "name": Node.name().is_in(["a", "b"]),
            "prop": Node.property("score") > 15,
            "window": Graph.window(3, 12),
            "before": Graph.before(12),
            "layer": Graph.layer("work"),
        }
        assert set(atoms) == set(views) | set(preds), "every atom needs a reference"
        every = names(graph)

        def selects(name, g):
            """What one atom selects on `g`: a view's members, or a predicate's passes."""
            return names(views[name](g)) if name in views else preds[name](g)

        def scope(name, g):
            """The graph the *next* operand of a conjunction is read against."""
            return views[name](g) if name in views else g

        # The complement of each atom, from the same independent references.
        complement = {
            "name": every - preds["name"](graph),
            "prop": every - preds["prop"](graph),
            "window": names(graph.before(3)) | names(graph.after(11)),
            "before": names(graph.after(11)),
            "layer": names(graph.exclude_layers(["work"])),
        }
        assert set(complement) == set(atoms), "every atom needs a negated reference"

        # A union or negation restricting time on one side and layers on the
        # other has no single (time, layers) answer, so it is rejected rather
        # than widened into a hull that admits what neither side does.
        time_views, layer_views = {"window", "before"}, {"layer"}

        def is_refused(a, b):
            return (a in time_views and b in layer_views) or (
                a in layer_views and b in time_views
            )

        cases, refused = [], []
        for a, b in combinations(atoms, 2):
            # `&`: apply the views in order, then read both operands there.
            composed = scope(b, scope(a, graph))
            cases.append(
                (
                    f"{a} & {b}",
                    atoms[a] & atoms[b],
                    names(composed) & selects(a, composed) & selects(b, composed),
                )
            )
            # `|`: each operand is read against the base graph and the results
            # are unioned, since neither branch constrains the other.
            if is_refused(a, b):
                refused.append((f"{a} | {b}", lambda a=a, b=b: atoms[a] | atoms[b]))
            else:
                cases.append(
                    (
                        f"{a} | {b}",
                        atoms[a] | atoms[b],
                        selects(a, graph) | selects(b, graph),
                    )
                )
        for a in atoms:
            cases.append((f"~{a}", ~atoms[a], complement[a]))

        mismatches = []
        for label, expr, want in cases:
            for path, got in (
                ("nodes[]", frozenset(graph.nodes[expr].name)),
                ("filter()", frozenset(graph.filter(expr).nodes.name)),
            ):
                if got != want:
                    mismatches.append(
                        f"[{path}] {label}: got {sorted(got)} want {sorted(want)}"
                    )
        assert not mismatches, "\n".join(mismatches)

        for label, build in refused:
            with pytest.raises(Exception, match="both time and layers"):
                graph.nodes[build()]
            with pytest.raises(Exception, match="both time and layers"):
                graph.filter(build())

    return check
