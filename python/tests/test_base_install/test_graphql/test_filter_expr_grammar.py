"""The filter tree as a GraphQL input.

`GqlFilter.expr` carries the same tree the local engine compiles: one grammar
for nodes, edges and views, with an expression on *both* sides of a comparison.
These tests send trees as JSON variables and check the answers against the
same graph read locally, so the wire grammar is pinned by results, not by
shape. The legacy grammar is converted onto the tree on arrival; one case
sends the same filter both ways and expects the same answer.
"""

from raphtory import Graph, filter as f
from utils import graphql_client


def build():
    """alice.score 3@0 7@2 9@6 · bob.score 5@1 2@7 · carol none · dave.score 1@2 1@3
    alice→bob [knows] @1 @4 · bob→carol [works] @2 · carol→dave [knows] @6"""
    g = Graph()
    for t, name, score in [
        (0, "alice", 3.0),
        (2, "alice", 7.0),
        (6, "alice", 9.0),
        (1, "bob", 5.0),
        (7, "bob", 2.0),
        (2, "dave", 1.0),
        (3, "dave", 1.0),
    ]:
        g.add_node(t, name, properties={"score": score})
    g.add_node(0, "carol")
    g.add_edge(1, "alice", "bob", layer="knows")
    g.add_edge(4, "alice", "bob", layer="knows")
    g.add_edge(2, "bob", "carol", layer="works")
    g.add_edge(6, "carol", "dave", layer="knows")
    return g


# `graph.filter(expr)` narrows membership; `nodes.filter(expr)` is the deferred
# form that keeps every node and narrows what each one sees, so membership
# questions are asked at the graph.
NODES = """
query($f: GqlFilter!) {
  graph(path: "g") { filter(expr: $f) { nodes { list { name } } } }
}
"""

EDGES = """
query($f: GqlFilter!) {
  graph(path: "g") { filter(expr: $f) { edges { list { src { name } dst { name } } } } }
}
"""


def read(t):
    return {"read": {"entity": "NODE", "target": t}}


def const(v):
    return {"const": v}


def node_names(client, tree):
    out = client.query(NODES, {"f": {"expr": tree}})
    return sorted(n["name"] for n in out["graph"]["filter"]["nodes"]["list"])


def edge_pairs(client, tree):
    out = client.query(EDGES, {"f": {"expr": tree}})
    return sorted(
        (e["src"]["name"], e["dst"]["name"])
        for e in out["graph"]["filter"]["edges"]["list"]
    )


def test_both_sides_of_a_comparison_are_expressions():
    """`degree > in_degree` has no constant, so the legacy grammar could not say
    it. The tree can, and the server answers what the local engine answers."""
    g = build()
    tree = {"gt": {"lhs": read({"degree": "BOTH"}), "rhs": read({"degree": "IN"})}}
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["alice", "bob", "carol"]
    local = sorted(n.name for n in g.filter(f.Node.degree() > f.Node.in_degree()).nodes)
    assert local == ["alice", "bob", "carol"]


def test_views_belong_to_the_read():
    """Inside [0, 5) alice's latest score is 7 and bob's is 5."""
    g = build()
    windowed = {
        "read": {
            "entity": "NODE",
            "views": [{"window": {"start": 0, "end": 5}}],
            "target": {"property": "score"},
        }
    }
    tree = {"gt": {"lhs": windowed, "rhs": const({"f64": 4.0})}}
    plain = {"gt": {"lhs": read({"property": "score"}), "rhs": const({"f64": 4.0})}}
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["alice", "bob"]
        assert node_names(client, plain) == ["alice"]


def test_temporal_aggregates_and_qualifiers():
    g = build()
    history = {"temporal": read({"property": "score"})}
    total = {"gt": {"lhs": {"sum": history}, "rhs": const({"f64": 10.0})}}
    any_high = {"gt": {"lhs": {"any": history}, "rhs": const({"f64": 4.0})}}
    two_updates = {"eq": {"lhs": {"len": history}, "rhs": const({"u64": 2})}}
    with graphql_client(g) as client:
        assert node_names(client, total) == ["alice"]
        assert node_names(client, any_high) == ["alice", "bob"]
        assert node_names(client, two_updates) == ["bob", "dave"]


def test_legacy_and_tree_spellings_agree():
    g = build()
    legacy = {"node": {"property": {"name": "score", "where": {"gt": {"f64": 4.0}}}}}
    tree = {"expr": {"gt": {"lhs": read({"property": "score"}), "rhs": const({"f64": 4.0})}}}
    with graphql_client(g) as client:
        via_legacy = client.query(NODES, {"f": legacy})
        via_tree = client.query(NODES, {"f": tree})
    assert via_legacy == via_tree
    assert [n["name"] for n in via_tree["graph"]["filter"]["nodes"]["list"]] == ["alice"]


def test_edge_reads_through_an_endpoint_keep_the_edge_views():
    """The window on the edge scopes the source node's score: inside [0, 5)
    alice's latest score is 7, so asking for the later 9 matches nothing."""
    g = build()
    src_score = {
        "read": {
            "entity": "EDGE",
            "endpoint": "SRC",
            "views": [{"window": {"start": 0, "end": 5}}],
            "target": {"property": "score"},
        }
    }
    late = {"eq": {"lhs": src_score, "rhs": const({"f64": 9.0})}}
    early = {"eq": {"lhs": src_score, "rhs": const({"f64": 7.0})}}
    with graphql_client(g) as client:
        assert edge_pairs(client, late) == []
        assert edge_pairs(client, early) == [("alice", "bob")]


def test_structural_predicates_and_views():
    g = build()
    works = {"isActive": {"entity": "EDGE", "views": [{"layers": ["works"]}]}}
    window_then_latest = {"view": [{"window": {"start": 0, "end": 5}}, {"latest": True}]}
    with graphql_client(g) as client:
        assert edge_pairs(client, works) == [("bob", "carol")]
        assert edge_pairs(client, window_then_latest) == [("alice", "bob")]


def test_combinators_presence_and_membership():
    g = build()
    tree = {
        "and": [
            {"isSome": read({"property": "score"})},
            {"not": {"startsWith": {"lhs": read({"field": "NAME"}), "rhs": const({"str": "a"})}}},
        ]
    }
    members = {"isIn": {"expr": read({"field": "NAME"}), "values": [{"str": "alice"}, {"str": "dave"}]}}
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["bob", "dave"]
        assert node_names(client, members) == ["alice", "dave"]
