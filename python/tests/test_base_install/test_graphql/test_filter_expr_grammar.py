"""The filter tree as a GraphQL input.

`FilterExpr` is the same tree the local engine compiles: the entity is the key
(`node`, `edge`, `explodedEdge`), each term on it is a `read` that carries its
own views, operators are enum values, and an expression stands on *both* sides
of a comparison. These tests send trees as
JSON variables and check the answers against the same graph read locally, so
the wire grammar is pinned by results, not by shape.
"""

import pytest
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
query($f: FilterExpr!) {
  graph(path: "g") { filter(expr: $f) { nodes { list { name } } } }
}
"""

EDGES = """
query($f: FilterExpr!) {
  graph(path: "g") { filter(expr: $f) { edges { list { src { name } dst { name } } } } }
}
"""


def node(expr):
    return {"node": expr}


def edge(expr):
    return {"edge": expr}


def const(v):
    return {"const": v}


def read(term, views=None):
    """A read of one term (`{"property": "score"}`, `{"field": "NAME"}`, ...),
    under `views` when given."""
    ((kind, value),) = term.items()
    payload = {"expr" if kind in ("src", "dst") else "name": value}
    if views is not None:
        payload["views"] = views
    return {"read": {kind: payload}}


def cmp(op, lhs, rhs):
    return {"cmp": {"op": op, "lhs": lhs, "rhs": rhs}}


def node_names(client, tree):
    out = client.query(NODES, {"f": tree})
    return sorted(n["name"] for n in out["graph"]["filter"]["nodes"]["list"])


def edge_pairs(client, tree):
    out = client.query(EDGES, {"f": tree})
    return sorted(
        (e["src"]["name"], e["dst"]["name"])
        for e in out["graph"]["filter"]["edges"]["list"]
    )


def test_both_sides_of_a_comparison_are_expressions():
    """`degree > in_degree` has no constant on either side, which a
    constant-only grammar could not say. The tree can, and the server answers
    what the local engine answers."""
    g = build()
    tree = node(cmp("GT", read({"field": "DEGREE"}), read({"field": "IN_DEGREE"})))
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["alice", "bob", "carol"]
    local = sorted(n.name for n in g.filter(f.Node.degree() > f.Node.in_degree()).nodes)
    assert local == ["alice", "bob", "carol"]


def test_views_belong_to_the_term():
    """Inside [0, 5) alice's latest score is 7 and bob's is 5."""
    g = build()
    windowed = read({"property": "score"}, [{"window": {"start": 0, "end": 5}}])
    tree = node(cmp("GT", windowed, const({"f64": 4.0})))
    plain = node(cmp("GT", read({"property": "score"}), const({"f64": 4.0})))
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["alice", "bob"]
        assert node_names(client, plain) == ["alice"]


def test_temporal_aggregates_and_qualifiers():
    g = build()
    history = read({"temporalProperty": "score"})

    def agg(op):
        return {"agg": {"op": op, "expr": history}}

    total = node(cmp("GT", agg("SUM"), const({"f64": 10.0})))
    # The qualifier follows the comparison: one answer per update, any must hold.
    any_high = node(
        {
            "quantified": {
                "op": "ANY",
                "expr": cmp("GT", history, const({"f64": 4.0})),
            }
        }
    )
    two_updates = node(cmp("EQ", agg("LEN"), const({"u64": 2})))
    # earliest / latest are updates of the history, not reductions of it.
    started_low = node(cmp("LT", agg("EARLIEST"), const({"f64": 2.0})))
    ended_low = node(cmp("LT", agg("LATEST"), const({"f64": 3.0})))
    with graphql_client(g) as client:
        assert node_names(client, total) == ["alice"]
        assert node_names(client, any_high) == ["alice", "bob"]
        assert node_names(client, two_updates) == ["bob", "dave"]
        assert node_names(client, started_low) == ["dave"]
        assert node_names(client, ended_low) == ["bob", "dave"]


def test_edge_reads_through_an_endpoint_keep_the_edge_views():
    """The window on the edge scopes the source node's score: inside [0, 5)
    alice's latest score is 7, so asking for the later 9 matches nothing."""
    g = build()
    src_score = read(
        {"src": read({"property": "score"}, [{"window": {"start": 0, "end": 5}}])}
    )
    late = edge(cmp("EQ", src_score, const({"f64": 9.0})))
    early = edge(cmp("EQ", src_score, const({"f64": 7.0})))
    with graphql_client(g) as client:
        assert edge_pairs(client, late) == []
        assert edge_pairs(client, early) == [("alice", "bob")]


def test_a_view_leg_restricts_the_whole_filter():
    """`and: [view, predicate]` applies the view first and the predicate inside it,
    like `graph.window(0, 2).filter(predicate)`: dave's updates at 2 and 3 fall
    outside [0, 2), so he is gone before the predicate runs."""
    g = build()
    window = {"view": [{"window": {"start": 0, "end": 2}}]}
    has_score = node(
        {"presence": {"op": "IS_SOME", "expr": read({"property": "score"})}}
    )
    with graphql_client(g) as client:
        assert node_names(client, {"and": [window, has_score]}) == ["alice", "bob"]
        assert node_names(client, has_score) == ["alice", "bob", "dave"]
        for shape in ({"or": [window, has_score]}, {"not": window}):
            with pytest.raises(Exception, match="view"):
                node_names(client, shape)


def test_structural_predicates_and_views():
    g = build()
    works = edge(read({"field": "IS_ACTIVE"}, [{"layers": ["works"]}]))
    window_then_latest = {
        "view": [{"window": {"start": 0, "end": 5}}, {"kind": "LATEST"}]
    }
    with graphql_client(g) as client:
        assert edge_pairs(client, works) == [("bob", "carol")]
        assert edge_pairs(client, window_then_latest) == [("alice", "bob")]


def test_combinators_presence_and_membership():
    g = build()
    tree = node(
        {
            "and": [
                {"presence": {"op": "IS_SOME", "expr": read({"property": "score"})}},
                {
                    "not": {
                        "str": {
                            "op": "STARTS_WITH",
                            "lhs": read({"field": "NAME"}),
                            "rhs": const({"str": "a"}),
                        }
                    }
                },
            ]
        }
    )
    members = node(
        {
            "isIn": {
                "expr": read({"field": "NAME"}),
                "values": {"list": [{"str": "alice"}, {"str": "dave"}]},
            }
        }
    )
    with graphql_client(g) as client:
        assert node_names(client, tree) == ["bob", "dave"]
        assert node_names(client, members) == ["alice", "dave"]


def test_layer_exclusion_default_layer_and_shrinks_read_as_the_graph_views():
    """Each view op is the graph view of the same name applied to the view so
    far, both as a whole-filter view and as the scope of an edge term. One
    extra edge dave→alice @3 sits on the default layer."""
    g = build()
    g.add_edge(3, "dave", "alice")
    cases = [
        ([{"kind": "DEFAULT_LAYER"}], g.default_layer(), f.Edge.default_layer()),
        (
            [{"excludeLayer": "knows"}],
            g.exclude_layer("knows"),
            f.Edge.exclude_layer("knows"),
        ),
        (
            [{"excludeLayers": ["knows", "works"]}],
            g.exclude_layers(["knows", "works"]),
            f.Edge.exclude_layers(["knows", "works"]),
        ),
        ([{"shrinkEnd": 4}], g.shrink_end(4), f.Edge.shrink_end(4)),
        (
            [{"window": {"start": 0, "end": 7}}, {"shrinkStart": 2}],
            g.window(0, 7).shrink_start(2),
            f.Edge.window(0, 7).shrink_start(2),
        ),
    ]
    with graphql_client(g) as client:
        for views, local_view, local_scope in cases:
            want = sorted((e.src.name, e.dst.name) for e in local_view.edges)
            assert edge_pairs(client, {"view": views}) == want, views
            active = edge(read({"field": "IS_ACTIVE"}, views))
            local = sorted(
                (e.src.name, e.dst.name)
                for e in g.filter(local_scope.is_active()).edges
            )
            assert edge_pairs(client, active) == local, views


def test_node_set_views_and_valid_read_as_the_graph_views():
    """`excludeNodes`, `subgraph`, `subgraphNodeTypes` and `kind: VALID` are the graph
    views of the same name applied to the view so far, both as a whole-filter
    view and as the scope of a node or edge term. alice and bob are `person`,
    carol is `org`; bob→carol is deleted @5, so it is not valid afterwards."""
    g = build()
    g.add_node(0, "alice", node_type="person")
    g.add_node(1, "bob", node_type="person")
    g.add_node(0, "carol", node_type="org")
    g.delete_edge(5, "bob", "carol", layer="works")
    cases = [
        (
            [{"excludeNodes": ["bob", "nobody"]}],
            g.exclude_nodes(["bob", "nobody"]),
            f.Node.exclude_nodes(["bob", "nobody"]),
            f.Edge.exclude_nodes(["bob", "nobody"]),
        ),
        (
            [{"subgraph": ["alice", "bob", "carol"]}],
            g.subgraph(["alice", "bob", "carol"]),
            f.Node.subgraph(["alice", "bob", "carol"]),
            f.Edge.subgraph(["alice", "bob", "carol"]),
        ),
        (
            [{"subgraphNodeTypes": ["person", "org"]}],
            g.subgraph_node_types(["person", "org"]),
            f.Node.subgraph_node_types(["person", "org"]),
            f.Edge.subgraph_node_types(["person", "org"]),
        ),
        ([{"kind": "VALID"}], g.valid(), f.Node.valid(), f.Edge.valid()),
        (
            [{"window": {"start": 0, "end": 5}}, {"excludeNodes": ["alice"]}],
            g.window(0, 5).exclude_nodes(["alice"]),
            f.Node.window(0, 5).exclude_nodes(["alice"]),
            f.Edge.window(0, 5).exclude_nodes(["alice"]),
        ),
    ]
    with graphql_client(g) as client:
        for views, local_view, node_scope, edge_scope in cases:
            want = sorted((e.src.name, e.dst.name) for e in local_view.edges)
            assert edge_pairs(client, {"view": views}) == want, views
            assert node_names(client, {"view": views}) == sorted(
                local_view.nodes.name
            ), views
            active = edge(read({"field": "IS_ACTIVE"}, views))
            local = sorted(
                (e.src.name, e.dst.name) for e in g.filter(edge_scope.is_active()).edges
            )
            assert edge_pairs(client, active) == local, views
            degree = node(
                cmp("GT", read({"field": "DEGREE"}, views), const({"u64": 0}))
            )
            local = sorted(g.filter(node_scope.degree() > 0).nodes.name)
            assert node_names(client, degree) == local, views


def test_node_ids_keep_their_type_in_a_view():
    """A node id in `excludeNodes`/`subgraph` is the `NodeId` scalar: an integer
    names an integer-indexed node."""
    g = Graph()
    for t, src, dst in [(1, 1, 2), (2, 2, 3), (3, 3, 1)]:
        g.add_edge(t, src, dst)
    with graphql_client(g) as client:
        assert node_names(client, {"view": [{"excludeNodes": [2]}]}) == ["1", "3"]
        assert node_names(client, {"view": [{"subgraph": [1, 2]}]}) == ["1", "2"]


def test_a_view_without_argument_is_named_by_kind():
    """`latest`, `snapshotLatest`, `valid` and `defaultLayer` are values of
    `kind`, so a view that is named but not applied cannot be spelled: the old
    boolean field is unknown, and so is a kind outside the enum."""
    g = build()
    with graphql_client(g) as client:
        for old in [True, False]:
            with pytest.raises(
                Exception,
                match='argument "expr.view.0", unknown field "valid" of type "ViewOp"',
            ):
                client.query(NODES, {"f": {"view": [{"valid": old}]}})
        with pytest.raises(
            Exception,
            match='argument "expr.view.0.kind", enumeration type "ViewKind" '
            'does not contain the value "VALIDATE"',
        ):
            client.query(NODES, {"f": {"view": [{"kind": "VALIDATE"}]}})


def test_empty_legs_are_refused():
    """An `and`, `or` or view leg with nothing under it is refused wherever it
    sits; in particular an empty `and`, and `not` of it, are not "everything"."""
    window = {"view": [{"window": {"start": 0, "end": 5}}]}
    with graphql_client(build()) as client:
        for shape, message in [
            ({"and": []}, "`and` needs at least one operand"),
            ({"not": {"and": []}}, "`and` needs at least one operand"),
            ({"or": []}, "`or` needs at least one operand"),
            ({"and": [{"view": []}, window]}, "a view filter needs at least one view"),
        ]:
            with pytest.raises(Exception, match=message):
                node_names(client, shape)
