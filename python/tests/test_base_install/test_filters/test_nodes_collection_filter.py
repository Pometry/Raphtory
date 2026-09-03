"""`.filter()` keeps every member and restricts it to the filtered view; `[...]` selects the
members that pass and returns them unfiltered. The two must be consistent across every filter
field — a filter with an enumerable domain (name/id) used to wrongly drop members from `.filter()`
while a property filter kept them."""

from itertools import combinations

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
        single = {n: frozenset(graph.nodes[e].name) for n, e in atoms.items()}
        every = frozenset(graph.nodes.name)
        cases = []
        for a, b in combinations(atoms, 2):
            cases.append((f"{a} & {b}", atoms[a] & atoms[b], single[a] & single[b]))
            cases.append((f"{a} | {b}", atoms[a] | atoms[b], single[a] | single[b]))
        for a in atoms:
            cases.append((f"~{a}", ~atoms[a], every - single[a]))
        views = {"window", "before", "layer"}

        def filter_path_reliable(label):
            # `graph.filter()` goes through the entity-filter path, which fails open on the same
            # composite classes as edge collections: `|` with a view, view & view, and `~view`.
            # `nodes[...]` is immune, so it is asserted for everything.
            if label.startswith("~"):
                return label[1:] not in views
            a, op, b = label.split(" ")
            if op == "|":
                return a not in views and b not in views
            return not (a in views and b in views)

        mismatches = []
        for label, expr, want in cases:
            if frozenset(graph.nodes[expr].name) != want:
                mismatches.append(f"[nodes[]] {label}")
            if filter_path_reliable(label):
                if frozenset(graph.filter(expr).nodes.name) != want:
                    mismatches.append(f"[filter()] {label}")
        assert not mismatches, mismatches
        # Pin the skip: when the entity path is fixed these fire — delete `filter_path_reliable`.
        assert frozenset(graph.filter(~atoms["window"]).nodes.name) != every - single["window"]
        assert (
            frozenset(graph.filter(atoms["name"] | atoms["window"]).nodes.name)
            != single["name"] | single["window"]
        )

    return check
