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
    """A representative subset of filter families combined with `&`/`|`/`~`: node collections
    (unlike edge collections today) obey set algebra for all of them."""

    def check(graph):
        atoms = {
            "name": Node.name().is_in(["a", "b"]),
            "prop": Node.property("score") > 15,
            "window": Graph.window(3, 12),
            "before": Graph.before(12),
            "layer": Graph.layer("work"),
        }
        # References computed without `nodes[...]`, which is the thing under
        # test: views come from the equivalent chained view, predicates are
        # evaluated over the collection. Reading them back through the subscript
        # would make these expectations agree with it by construction.
        single = {
            "name": frozenset({"a", "b"}) & frozenset(graph.nodes.name),
            "prop": frozenset(
                n.name for n in graph.nodes if (n.properties.get("score") or 0) > 15
            ),
            "window": frozenset(graph.window(3, 12).nodes.name),
            "before": frozenset(graph.before(12).nodes.name),
            "layer": frozenset(graph.layer("work").nodes.name),
        }
        assert set(single) == set(atoms), "every atom needs an independent reference"
        every = frozenset(graph.nodes.name)
        cases = []
        views = {"window", "before", "layer"}
        # On `nodes[]` a view expression is an existence test, so it follows set algebra under `&`
        # and `|`. On `filter()` each expression is read on the base graph (pinned below), and
        # `|` or `~` on a view is refused.
        select_only = set()
        for a, b in combinations(atoms, 2):
            cases.append((f"{a} & {b}", atoms[a] & atoms[b], single[a] & single[b]))
            cases.append((f"{a} | {b}", atoms[a] | atoms[b], single[a] | single[b]))
            if a in views or b in views:
                select_only.update({f"{a} & {b}", f"{a} | {b}"})
        # `~` of a view is "exists in the graph `~` keeps", which has no graph yet;
        # "does not exist in the view" is `~` of its `.exists()` test
        exists = {
            "window": Node.window(3, 12).exists(),
            "before": Node.before(12).exists(),
            "layer": Node.layer("work").exists(),
        }
        for a in atoms:
            if a in views:
                with pytest.raises(Exception, match="exists"):
                    graph.nodes[~atoms[a]]
                cases.append((f"~{a}.exists()", ~exists[a], every - single[a]))
            else:
                cases.append((f"~{a}", ~atoms[a], every - single[a]))

        mismatches = []
        for label, expr, want in cases:
            if frozenset(graph.nodes[expr].name) != want:
                mismatches.append(f"[nodes[]] {label}")
            if (
                label not in select_only
                and frozenset(graph.filter(expr).nodes.name) != want
            ):
                mismatches.append(f"[filter()] {label}")
        assert not mismatches, mismatches
        # Each expression is read on the base graph; to filter inside a window use
        # graph.window(..).filter(..). On `nodes[]` the view is one test among the others:
        # c exists in the window through the edge at 10, and its score of 30 is read on the
        # graph, so `window & prop` keeps b and c.
        view_ref = {
            "window": graph.window(3, 12),
            "before": graph.before(12),
            "layer": graph.layer("work"),
        }
        for v, viewed in view_ref.items():
            for name in ("name", "prop"):
                both = frozenset(viewed.nodes.name) & frozenset(
                    graph.filter(atoms[name]).nodes.name
                )
                assert (
                    frozenset(graph.filter(atoms[v] & atoms[name]).nodes.name) == both
                ), f"{v} & {name}"
                exists_and = frozenset(viewed.nodes.name) & frozenset(
                    graph.nodes[atoms[name]].name
                )
                assert (
                    frozenset(graph.nodes[atoms[v] & atoms[name]].name) == exists_and
                ), f"{v} & {name}"
        assert sorted(graph.nodes[atoms["window"] & atoms["prop"]].name) == ["b", "c"]
        assert frozenset(
            graph.filter(atoms["window"] & atoms["layer"]).nodes.name
        ) == frozenset(graph.window(3, 12).layer("work").nodes.name)
        # `|` on a view: a plain test on `nodes[]`, refused on `filter()`; `~` on a view is
        # refused on both, and "not in the window" is `~` of the `.exists()` test
        assert (
            frozenset(graph.nodes[~exists["window"]].name) == every - single["window"]
        )
        with pytest.raises(Exception, match="exists"):
            graph.nodes[~atoms["window"]]
        for expr in (~atoms["window"], atoms["name"] | atoms["window"]):
            with pytest.raises(Exception, match="view"):
                graph.filter(expr)

    return check


def _five(graph):
    for name in ["a", "b", "c", "d", "e"]:
        graph.add_node(1, name)
    return graph


@with_variants(_five)
def test_combined_subgraphs_select_their_set_algebra():
    """`&` of subgraphs keeps their intersection, `|` their union, and `&` of a
    subgraph and a predicate keeps what passes both."""

    def check(graph):
        ab = Graph.subgraph(["a", "b"])
        bc = Graph.subgraph(["b", "c"])
        assert sorted(graph.nodes[ab & bc].id) == ["b"]
        assert sorted(graph.nodes[ab | bc].id) == ["a", "b", "c"]
        assert sorted(graph.nodes[ab & (Node.name() == "a")].id) == ["a"]

    return check


def _early_and_late(graph):
    graph.add_node(1, "a")
    graph.add_node(10, "b")
    return graph


@with_variants(_early_and_late)
def test_a_time_view_under_and_restricts_like_the_graph_view():
    """A time view under `&` with a node predicate keeps what the graph
    view of the same name keeps, then applies the predicate inside it.

    time        1    5    10
    a           ●
    b                     ●
    after(5)         (=========
    """

    def check(graph):
        names = Node.name().is_in(["a", "b"])
        legs = [
            (Graph.at(1), graph.at(1)),
            (Graph.before(5), graph.before(5)),
            (Graph.after(5), graph.after(5)),
            (Graph.latest(), graph.latest()),
            (Graph.snapshot_at(5), graph.snapshot_at(5)),
        ]
        for leg, viewed in legs:
            assert sorted(graph.filter(leg & names).nodes.name) == sorted(
                viewed.filter(names).nodes.name
            ), leg

    return check


@with_variants(_early_and_late, variants=["graph"])
def test_after_under_and_drops_what_came_before():
    """On the event graph above, `after(5) & name in [a, b]` keeps b only."""

    def check(graph):
        names = Node.name().is_in(["a", "b"])
        assert sorted(graph.filter(Graph.after(5) & names).nodes.name) == ["b"]

    return check


@with_variants(_rich)
def test_a_filtered_count_matches_what_is_listed():
    # A node filter's count, length and iteration agree, and match
    # the collection form. Every node here has its own update, so none leaves
    # the filtered graph by losing its edges (#2821).
    def check(graph):
        for expr in (
            filter.Node.name() == "a",
            filter.Node.name().is_in(["a", "c"]),
            filter.Node.id() == "a",
        ):
            view = graph.filter(expr)
            listed = len(list(view.nodes))
            assert view.count_nodes() == listed, expr
            assert len(view.nodes) == listed, expr
            assert len(graph.nodes[expr]) == listed, expr

    return check
