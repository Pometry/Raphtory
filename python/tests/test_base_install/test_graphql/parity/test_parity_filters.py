"""Filter-expression parity: one `filter(expr)` entry point, two backends.

Filtering is the one place where the local `Graph` and `RemoteGraph` are asked
to agree on a *program*, not just a call. Locally a `raphtory.filter`
expression is handed straight to the engine; remotely the very same Python
object has to be lowered to a GraphQL `GqlFilter`, shipped, re-parsed and
re-planned on the server. Every step of that lowering can drop a conjunct,
confuse a property source (metadata vs temporal), invert a comparison, or
attach a view scope to the wrong subtree — and still return a plausible answer.

So each case here builds one expression, applies it to *both* graphs, and
compares what the filtered handle reports:

* `test_expr_parity` — the local and remote answers must match, per expression.
* `test_expr_discriminates` — asserted **per side**: the expression must leave
  a non-empty result that differs from the unfiltered handle. A backend that
  ignored every filter would agree with itself and sail through the parity
  matrix; this is the guard that makes the matrix mean something. Expressions
  that cannot discriminate on this graph are not quietly excused — they are
  listed in `_UNIVERSAL_EXPRS` with the reason, and asserted to select
  *everything* on both sides, which is a claim about behaviour rather than a
  hole in the suite.
* `test_site_matrix_parity` / `test_site_matrix_discriminates` — the same
  expressions again, crossed with every handle that accepts a filter
  (`graph`, `nodes`, `node`, `PathFromNode`, `PathFromGraph`, and the
  `collection[expr]` sugar), because a filter can be lowered correctly for one
  application site and dropped at another.

Divergences found by this module are recorded in `KNOWN_GAPS` and replayed as
strict xfails at the bottom, so they cannot be forgotten or silently fixed.
"""

import pytest

from _parity import KNOWN_GAPS, assert_parity, canonical, graph_pair
from raphtory import filter as f

# --- fixture ----------------------------------------------------------------


def _build_filters(g):
    """One graph on which every filter in this module is *discriminating*.

    Shaped deliberately so that no comparison can pass by accident:

    * **degrees spread 0..4** (`iso` 0, `spoke2`/`spoke3` low, `hub` highest),
      so `degree()`/`in_degree()`/`out_degree()` comparisons each keep some
      nodes and drop others;
    * **numeric, string and boolean properties** (`score`, `tag`, `live`) with
      values spread across the comparison points used below, and two nodes
      carrying *no* `score` at all so `is_some`/`is_none` bite;
    * **`level` exists twice** — as node metadata (`gold`/`silver`) and as a
      temporal property (`bronze`) — so a lowering that confuses the two
      sources gives a visibly different answer;
    * **`score` is written twice** for `hub` and `spoke1`, so the temporal
      aggregations (`any`/`all`/`first`/`last`/`min`/`max`/`sum`) disagree with
      each other and with the aggregated `property()` read;
    * **three layers** (`knows`, `works`, `likes`) with events at t=2..12, so
      layer and window scopes each select a different slice, and a chain of the
      two is narrower than either;
    * **a self-loop** (`spoke2 -> spoke2`) so `is_self_loop()` matches
      something, and **a tombstone** on `hub -> spoke1` so `is_deleted()` does.
    """
    # Nodes: types, plus numeric / string / boolean temporal properties.
    g.add_node(
        1,
        "hub",
        node_type="user",
        properties={"score": 10, "tag": "alpha", "live": True},
    )
    g.add_node(
        2,
        "spoke1",
        node_type="user",
        properties={"score": 20, "tag": "beta", "live": False},
    )
    g.add_node(
        3,
        "spoke2",
        node_type="admin",
        properties={"score": 30, "tag": "alphabet", "live": True},
    )
    g.add_node(4, "spoke3", node_type="bot", properties={"tag": "gamma"})
    g.add_node(
        5,
        "leaf",
        node_type="admin",
        properties={"score": 5, "tag": "delta", "live": False},
    )
    g.add_node(6, "iso", node_type="bot", properties={"tag": "epsilon"})

    # A temporal property that shares its name with a metadata key below.
    g.add_node(7, "hub", properties={"level": "bronze"})
    g.add_node(7, "spoke1", properties={"level": "bronze"})

    # Node metadata: same key, different values from the temporal `level`.
    g.node("hub").add_metadata({"level": "gold", "region": "eu"})
    g.node("spoke1").add_metadata({"level": "silver", "region": "us"})
    g.node("spoke2").add_metadata({"level": "gold", "region": "eu"})
    g.node("leaf").add_metadata({"level": "silver", "region": "apac"})

    # Edges across three layers, with spread numeric and string properties.
    g.add_edge(
        2, "hub", "spoke1", properties={"weight": 1.5, "note": "aa"}, layer="knows"
    )
    g.add_edge(
        3, "hub", "spoke2", properties={"weight": 2.5, "note": "ab"}, layer="knows"
    )
    g.add_edge(
        4, "spoke3", "hub", properties={"weight": 3.5, "note": "bb"}, layer="works"
    )
    g.add_edge(
        5, "spoke1", "leaf", properties={"weight": 4.5, "note": "bc"}, layer="works"
    )
    g.add_edge(6, "leaf", "hub", layer="likes")
    g.add_edge(
        8,
        "spoke2",
        "spoke2",
        properties={"weight": 0.5, "note": "self"},
        layer="likes",
    )

    # Edge metadata, again distinct from the temporal edge properties.
    g.edge("hub", "spoke1").add_metadata({"kind": "strong"}, layer="knows")
    g.edge("hub", "spoke2").add_metadata({"kind": "weak"}, layer="knows")
    g.edge("spoke3", "hub").add_metadata({"kind": "strong"}, layer="works")

    # A tombstone, so the validity predicates have something to separate.
    g.delete_edge(9, "hub", "spoke1", layer="knows", event_id=None)

    # A second event on one edge, so exploded and aggregated reads differ.
    g.add_edge(
        10, "hub", "spoke2", properties={"weight": 9.5, "note": "zz"}, layer="knows"
    )

    # The final instant carries `score` for two nodes with different values and
    # an event on one edge, so `Node.latest()` / `Edge.latest()` — which scope
    # to the graph's last instant — have something to separate there.
    g.add_node(12, "hub", properties={"score": 100})
    g.add_node(12, "spoke1", properties={"score": 1})
    g.add_edge(
        12, "spoke1", "leaf", properties={"weight": 7.5, "note": "dd"}, layer="works"
    )


@pytest.fixture(scope="module")
def filter_pair():
    # One server for the whole module — every case below is read-only.
    with graph_pair(_build_filters) as pair:
        yield pair


# --- probes -----------------------------------------------------------------
#
# A probe reduces a filtered handle to a keyed, fully-labelled structure. Keys
# matter: `canonical` sorts the members of an unkeyed tuple, which would let a
# swapped `src`/`dst` or a swapped `in_degree`/`out_degree` compare equal. Dicts
# keyed by entity identity and by fact name cannot be reordered into agreement.
#
# The facts are also chosen to be sensitive to both things a filter can change:
# *membership* (which entities remain) and *scope* (what each survivor answers,
# since a collection filter narrows the view its members report through rather
# than the collection itself).


def _node_facts(n):
    return {
        "degree": n.degree(),
        "in_degree": n.in_degree(),
        "out_degree": n.out_degree(),
        "earliest": n.earliest_time,
        "latest": n.latest_time,
    }


def _edge_facts(e):
    return {
        "layers": sorted(e.layer_names),
        "earliest": e.earliest_time,
        "latest": e.latest_time,
        "valid": e.is_valid(),
        "deleted": e.is_deleted(),
    }


def _edge_key(e):
    # A single string, so `canonical` cannot reorder src and dst into agreement.
    return f"{e.src.name}->{e.dst.name}"


def _probe_graph(h):
    return {
        "nodes": {n.name: _node_facts(n) for n in h.nodes},
        "edges": {_edge_key(e): _edge_facts(e) for e in h.edges},
    }


def _probe_nodes(h):
    return {"nodes": {n.name: _node_facts(n) for n in h}}


def _probe_edges(h):
    return {"edges": {_edge_key(e): _edge_facts(e) for e in h}}


def _probe_node(h):
    """A single node handle; a filter may reduce it to ``None``."""
    return {"nodes": {} if h is None else {h.name: _node_facts(h)}}


def _probe_path_from_node(h):
    return {"nodes": {n.name: _node_facts(n) for n in h}}


def _probe_path_from_graph(h):
    # Flattened to `source|member` keys: keeps every row attributed to its
    # source without nesting that `canonical` would have to sort.
    return {
        "nodes": {
            f"{source.name}|{n.name}": _node_facts(n)
            for source, path in h
            for n in path
        }
    }


# --- expressions ------------------------------------------------------------
#
# Every entry is a zero-argument builder (expressions are cheap, and building
# them lazily keeps a construction-time TypeError attributable to its own case).

NODE_PROPERTY_EXPRS = {
    "node.prop.gt": lambda: f.Node.property("score") > 15,
    "node.prop.ge": lambda: f.Node.property("score") >= 20,
    "node.prop.lt": lambda: f.Node.property("score") < 25,
    "node.prop.le": lambda: f.Node.property("score") <= 20,
    "node.prop.eq": lambda: f.Node.property("score") == 30,
    "node.prop.ne": lambda: f.Node.property("score") != 30,
    "node.prop.is_in": lambda: f.Node.property("score").is_in([1, 30]),
    "node.prop.is_not_in": lambda: f.Node.property("score").is_not_in([1, 30]),
    "node.prop.is_some": lambda: f.Node.property("score").is_some(),
    "node.prop.is_none": lambda: f.Node.property("score").is_none(),
    "node.prop.bool_eq": lambda: f.Node.property("live") == True,  # noqa: E712
    "node.prop.bool_ne": lambda: f.Node.property("live") != True,  # noqa: E712
    "node.prop.contains": lambda: f.Node.property("tag").contains("lph"),
    "node.prop.not_contains": lambda: f.Node.property("tag").not_contains("lph"),
    "node.prop.starts_with": lambda: f.Node.property("tag").starts_with("alpha"),
    "node.prop.ends_with": lambda: f.Node.property("tag").ends_with("a"),
    "node.prop.fuzzy": lambda: f.Node.property("tag").fuzzy_search("alpho", 1, False),
}

NODE_FIELD_EXPRS = {
    "node.name.eq": lambda: f.Node.name() == "hub",
    "node.name.ne": lambda: f.Node.name() != "hub",
    "node.name.is_in": lambda: f.Node.name().is_in(["hub", "leaf"]),
    "node.name.is_not_in": lambda: f.Node.name().is_not_in(["hub", "leaf"]),
    "node.name.contains": lambda: f.Node.name().contains("spoke"),
    "node.name.not_contains": lambda: f.Node.name().not_contains("spoke"),
    "node.name.starts_with": lambda: f.Node.name().starts_with("s"),
    "node.name.ends_with": lambda: f.Node.name().ends_with("1"),
    "node.name.fuzzy": lambda: f.Node.name().fuzzy_search("hab", 1, False),
    "node.type.eq": lambda: f.Node.node_type() == "admin",
    "node.type.ne": lambda: f.Node.node_type() != "admin",
    "node.type.is_in": lambda: f.Node.node_type().is_in(["admin", "bot"]),
    "node.type.is_not_in": lambda: f.Node.node_type().is_not_in(["admin", "bot"]),
    "node.type.contains": lambda: f.Node.node_type().contains("o"),
    "node.type.not_contains": lambda: f.Node.node_type().not_contains("o"),
    "node.type.starts_with": lambda: f.Node.node_type().starts_with("a"),
    "node.type.ends_with": lambda: f.Node.node_type().ends_with("t"),
    "node.type.fuzzy": lambda: f.Node.node_type().fuzzy_search("usor", 1, False),
    "node.id.eq": lambda: f.Node.id() == "hub",
    "node.id.ne": lambda: f.Node.id() != "hub",
    "node.id.is_in": lambda: f.Node.id().is_in(["hub", "iso"]),
    "node.id.is_not_in": lambda: f.Node.id().is_not_in(["hub", "iso"]),
    "node.id.contains": lambda: f.Node.id().contains("spoke"),
    "node.id.not_contains": lambda: f.Node.id().not_contains("spoke"),
    "node.id.starts_with": lambda: f.Node.id().starts_with("s"),
    "node.id.ends_with": lambda: f.Node.id().ends_with("2"),
}

DEGREE_EXPRS = {
    "degree.gt": lambda: f.Node.degree() > 1,
    "degree.ge": lambda: f.Node.degree() >= 2,
    "degree.lt": lambda: f.Node.degree() < 2,
    "degree.le": lambda: f.Node.degree() <= 1,
    "degree.eq": lambda: f.Node.degree() == 1,
    "degree.ne": lambda: f.Node.degree() != 1,
    "degree.is_in": lambda: f.Node.degree().is_in([0, 4]),
    "degree.is_not_in": lambda: f.Node.degree().is_not_in([0, 4]),
    "in_degree.ge": lambda: f.Node.in_degree() >= 2,
    "in_degree.eq0": lambda: f.Node.in_degree() == 0,
    "out_degree.eq": lambda: f.Node.out_degree() == 1,
    "out_degree.gt": lambda: f.Node.out_degree() > 1,
}

# The three property sources. `level` is present as *both* metadata and a
# temporal property, with different values, so a lowering that reads the wrong
# source cannot produce the right answer (see `test_property_sources_are_distinct`).
SOURCE_EXPRS = {
    "source.node.metadata.eq": lambda: f.Node.metadata("level") == "gold",
    "source.node.metadata.ne": lambda: f.Node.metadata("level") != "gold",
    "source.node.metadata.is_in": lambda: f.Node.metadata("region").is_in(
        ["eu", "apac"]
    ),
    "source.node.metadata.is_some": lambda: f.Node.metadata("region").is_some(),
    "source.node.metadata.is_none": lambda: f.Node.metadata("region").is_none(),
    "source.node.metadata.contains": lambda: f.Node.metadata("region").contains("a"),
    "source.node.property.eq": lambda: f.Node.property("level") == "bronze",
    "source.node.temporal.any": lambda: f.Node.property("score").temporal().any() > 50,
    "source.node.temporal.all": lambda: f.Node.property("score").temporal().all() > 5,
    "source.node.temporal.first": (
        lambda: f.Node.property("score").temporal().first() > 15
    ),
    "source.node.temporal.last": (
        lambda: f.Node.property("score").temporal().last() > 15
    ),
    "source.node.temporal.min": lambda: f.Node.property("score").temporal().min() > 5,
    "source.node.temporal.max": lambda: f.Node.property("score").temporal().max() > 50,
    "source.node.temporal.sum": lambda: f.Node.property("score").temporal().sum() > 50,
    "source.edge.metadata.is_some": lambda: f.Edge.metadata("kind").is_some(),
    "source.edge.metadata.is_none": lambda: f.Edge.metadata("kind").is_none(),
    "source.edge.property.eq": lambda: f.Edge.property("note") == "zz",
    "source.edge.temporal.any": (
        lambda: f.Edge.property("weight").temporal().any() > 3.0
    ),
    "source.edge.temporal.avg": (
        lambda: f.Edge.property("weight").temporal().avg() > 3.0
    ),
    "source.edge.temporal.first": (
        lambda: f.Edge.property("weight").temporal().first() > 2.0
    ),
    "source.edge.temporal.last": (
        lambda: f.Edge.property("weight").temporal().last() > 2.0
    ),
}

EDGE_PROPERTY_EXPRS = {
    "edge.prop.gt": lambda: f.Edge.property("weight") > 2.0,
    "edge.prop.ge": lambda: f.Edge.property("weight") >= 2.5,
    "edge.prop.lt": lambda: f.Edge.property("weight") < 3.0,
    "edge.prop.le": lambda: f.Edge.property("weight") <= 2.5,
    "edge.prop.eq": lambda: f.Edge.property("weight") == 3.5,
    "edge.prop.ne": lambda: f.Edge.property("weight") != 3.5,
    "edge.prop.is_in": lambda: f.Edge.property("weight").is_in([1.5, 4.5]),
    "edge.prop.is_not_in": lambda: f.Edge.property("weight").is_not_in([1.5, 4.5]),
    "edge.prop.is_some": lambda: f.Edge.property("weight").is_some(),
    "edge.prop.is_none": lambda: f.Edge.property("weight").is_none(),
    "edge.prop.contains": lambda: f.Edge.property("note").contains("a"),
    "edge.prop.not_contains": lambda: f.Edge.property("note").not_contains("a"),
    "edge.prop.starts_with": lambda: f.Edge.property("note").starts_with("a"),
    "edge.prop.ends_with": lambda: f.Edge.property("note").ends_with("b"),
}

EDGE_ENDPOINT_EXPRS = {
    "edge.src.name.eq": lambda: f.Edge.src().name() == "hub",
    "edge.src.name.is_in": lambda: f.Edge.src().name().is_in(["hub", "leaf"]),
    "edge.src.name.starts_with": lambda: f.Edge.src().name().starts_with("s"),
    "edge.src.name.fuzzy": lambda: f.Edge.src().name().fuzzy_search("hab", 1, False),
    "edge.dst.name.eq": lambda: f.Edge.dst().name() == "hub",
    "edge.dst.name.is_not_in": lambda: f.Edge.dst().name().is_not_in(["hub"]),
    "edge.src.type.eq": lambda: f.Edge.src().node_type() == "user",
    "edge.dst.type.eq": lambda: f.Edge.dst().node_type() == "admin",
    "edge.src.id.eq": lambda: f.Edge.src().id() == "hub",
    "edge.dst.id.is_in": lambda: f.Edge.dst().id().is_in(["hub", "leaf"]),
    "edge.src.property.gt": lambda: f.Edge.src().property("score") > 15,
    "edge.dst.property.is_none": lambda: f.Edge.dst().property("level").is_none(),
    "edge.src.metadata.eq": lambda: f.Edge.src().metadata("region") == "eu",
    "edge.src.metadata.is_none": lambda: f.Edge.src().metadata("region").is_none(),
    "edge.dst.metadata.is_in": lambda: f.Edge.dst().metadata("region").is_in(["eu"]),
}

COMBINATOR_EXPRS = {
    "comb.and": lambda: (f.Node.property("score") > 5) & (f.Node.node_type() == "user"),
    "comb.or": lambda: (f.Node.name() == "iso") | (f.Node.node_type() == "admin"),
    "comb.not": lambda: ~(f.Node.node_type() == "admin"),
    "comb.not_and": lambda: ~(
        (f.Node.property("score") > 5) & (f.Node.node_type() == "user")
    ),
    # Two levels of nesting, mixing all three combinators.
    "comb.nested_2": lambda: (
        (f.Node.property("score") > 5) & (f.Node.node_type() == "user")
    )
    | (f.Node.name() == "leaf"),
    "comb.nested_3": lambda: (
        ((f.Node.property("score") >= 10) | (f.Node.node_type() == "bot"))
        & ~((f.Node.name() == "iso") | (f.Node.name() == "spoke3"))
    ),
    "comb.edge_or": lambda: (f.Edge.property("weight") > 4.0)
    | (f.Edge.metadata("kind") == "weak"),
    "comb.edge_not": lambda: ~(f.Edge.property("note").starts_with("a")),
    # Node and edge predicates in one expression — the headline capability.
    "comb.mixed_and": lambda: (f.Node.property("score") > 5)
    & (f.Edge.property("weight") > 2.0),
    "comb.mixed_not": lambda: ~(
        (f.Node.property("score") > 5) & (f.Edge.property("weight") > 2.0)
    ),
    "comb.mixed_nested": lambda: (
        (f.Node.node_type() == "user") | (f.Node.node_type() == "admin")
    )
    & ((f.Edge.property("weight") > 2.0) | f.Edge.property("weight").is_none()),
}

# Graph-level view filters: they move *when* and *where* the graph is
# evaluated, rather than testing a field.
VIEW_EXPRS = {
    "view.window": lambda: f.Graph.window(2, 5),
    "view.at": lambda: f.Graph.at(3),
    "view.before": lambda: f.Graph.before(4),
    "view.after": lambda: f.Graph.after(3),
    "view.latest": lambda: f.Graph.latest(),
    "view.layer": lambda: f.Graph.layer("knows"),
    "view.layers": lambda: f.Graph.layers(["knows", "works"]),
    "view.snapshot_at": lambda: f.Graph.snapshot_at(4),
    "view.chain_window_layers": lambda: f.Graph.window(2, 7).layers(["knows", "works"]),
    "view.chain_layer_at": lambda: f.Graph.layer("works").at(4),
    "view.chain_layers_snapshot": lambda: f.Graph.layers(["knows"]).snapshot_at(4),
    "view.chain_three": lambda: f.Graph.window(1, 8).layers(["knows", "works"]).at(4),
    "view.and_view": lambda: f.Graph.window(2, 7) & f.Graph.layer("knows"),
    "view.and_node": lambda: f.Graph.window(2, 7) & (f.Node.property("score") > 15),
    "view.and_edge": lambda: f.Graph.layers(["knows", "works"])
    & (f.Edge.property("weight") > 2.0),
    "view.and_mixed": lambda: f.Graph.window(2, 8)
    & (f.Node.node_type() != "bot")
    & f.Edge.property("weight").is_some(),
}

# View scopes attached to a node or edge predicate rather than to the graph:
# the predicate is evaluated through that scope only.
SCOPED_EXPRS = {
    "scoped.node.window": lambda: f.Node.window(1, 6).property("score") > 15,
    "scoped.node.at": lambda: f.Node.at(3).property("score") > 15,
    "scoped.node.before": lambda: f.Node.before(4).property("score") > 15,
    "scoped.node.after": lambda: f.Node.after(3).property("score") > 15,
    "scoped.node.latest": lambda: f.Node.latest().property("score") > 15,
    "scoped.node.snapshot_at": lambda: f.Node.snapshot_at(5).property("score") > 15,
    "scoped.node.snapshot_latest": (
        lambda: f.Node.snapshot_latest().property("score") > 15
    ),
    "scoped.node.layer": lambda: f.Node.layer("knows").property("score") > 15,
    "scoped.node.layers": lambda: f.Node.layers(["knows", "works"]).property("score")
    > 15,
    "scoped.node.metadata": lambda: f.Node.window(1, 6).metadata("region") == "eu",
    "scoped.node.is_active": lambda: f.Node.window(1, 3).is_active(),
    "scoped.edge.window": lambda: f.Edge.window(2, 5).property("weight") > 2.0,
    "scoped.edge.at": lambda: f.Edge.at(3).property("weight") > 2.0,
    "scoped.edge.before": lambda: f.Edge.before(4).property("weight") > 2.0,
    "scoped.edge.after": lambda: f.Edge.after(3).property("weight") > 2.0,
    "scoped.edge.latest": lambda: f.Edge.latest().property("weight") > 2.0,
    "scoped.edge.snapshot_at": lambda: f.Edge.snapshot_at(5).property("weight") > 2.0,
    "scoped.edge.layer": lambda: f.Edge.layer("knows").property("weight") > 2.0,
    "scoped.edge.layers": lambda: f.Edge.layers(["knows", "works"]).property("weight")
    > 2.0,
    "scoped.edge.metadata": lambda: f.Edge.layer("knows").metadata("kind") == "strong",
    "scoped.edge.is_valid": lambda: f.Edge.window(2, 4).is_valid(),
    "scoped.edge.is_deleted": lambda: f.Edge.window(2, 11).is_deleted(),
    "scoped.exploded.is_valid": lambda: f.ExplodedEdge.window(2, 4).is_valid(),
}

PREDICATE_EXPRS = {
    "pred.node.at.is_active": lambda: f.Node.at(3).is_active(),
    "pred.edge.is_self_loop": lambda: f.Edge.is_self_loop(),
    "pred.edge.is_deleted": lambda: f.Edge.is_deleted(),
    "pred.edge.layer.is_active": lambda: f.Edge.layer("knows").is_active(),
    "pred.exploded.is_deleted": lambda: f.ExplodedEdge.is_deleted(),
    "pred.exploded.is_self_loop": lambda: f.ExplodedEdge.is_self_loop(),
}

# Exploded-edge property reads: evaluated per event rather than per aggregated
# edge, so an edge survives when *some* event matches and is scoped to the
# matching events (`hub -> spoke2` carries weights 2.5 and 9.5, so `> 2.0`
# keeps it while `!= "zz"` on the note drops the 9.5 event). Unlike the plain
# edge metadata read — which is layer-keyed and matches nothing unqualified —
# the exploded metadata read resolves per event, so the equality bites.
EXPLODED_EXPRS = {
    "exploded.prop.gt": lambda: f.ExplodedEdge.property("weight") > 2.0,
    "exploded.prop.eq": lambda: f.ExplodedEdge.property("weight") == 3.5,
    "exploded.metadata.eq": lambda: f.ExplodedEdge.metadata("kind") == "strong",
    "exploded.comb.and": lambda: (f.ExplodedEdge.property("weight") > 2.0)
    & (f.ExplodedEdge.property("note") != "zz"),
}

EXPRS = {
    **NODE_PROPERTY_EXPRS,
    **NODE_FIELD_EXPRS,
    **DEGREE_EXPRS,
    **SOURCE_EXPRS,
    **EDGE_PROPERTY_EXPRS,
    **EDGE_ENDPOINT_EXPRS,
    **COMBINATOR_EXPRS,
    **VIEW_EXPRS,
    **SCOPED_EXPRS,
    **PREDICATE_EXPRS,
    **EXPLODED_EXPRS,
}


# --- parity -----------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(EXPRS), ids=sorted(EXPRS))
def test_expr_parity(filter_pair, name):
    """Local and remote agree on what an expression selects, through `filter`."""
    build = EXPRS[name]
    assert_parity(filter_pair, lambda g: _probe_graph(g.filter(build())))


# --- non-vacuity guard ------------------------------------------------------
#
# Asserted per side, local included. That is not re-testing the local engine,
# which this suite assumes works — it is testing that *this expression on this
# fixture* still has bite. Parity compares two answers, so an expression that
# matches everything (or nothing) makes both sides agree while proving nothing:
# a backend that ignored filters outright would return exactly the same thing.
# The local half is what notices, and what it usually catches is the case going
# stale as the fixture moves, not the engine breaking.


def _discriminating_axes(probed, baseline):
    """Axes on which `probed` kept some entities *and* differs from unfiltered.

    An axis (`nodes` / `edges`) counts when the filtered answer is non-empty —
    the filter kept something — and differs from the unfiltered answer — the
    filter dropped or rescoped something. A filter that matched everything
    fails the second half; one that matched nothing fails the first.
    """
    return [
        axis
        for axis, members in probed.items()
        if members and members != baseline.get(axis)
    ]


# Which axis each expression family predicates over. Requiring *that* axis to
# discriminate — not merely "some axis" — is what stops an edge filter matching
# zero edges from passing because the surviving nodes happened to be rescoped
# (their degrees drop when every edge is filtered out). Names are the single
# source of truth here, and `test_every_expr_declares_an_axis` keeps them honest.
_EDGE_AXIS_PREFIXES = (
    "edge.",
    "source.edge.",
    "scoped.edge.",
    "scoped.exploded.",
    "pred.edge.",
    "pred.exploded.",
    "comb.edge_",
    "exploded.",
)
_NODE_AXIS_PREFIXES = (
    "node.",
    "degree.",
    "in_degree.",
    "out_degree.",
    "source.node.",
    "scoped.node.",
    "pred.node.",
)
# Expressions constraining both entity types, or only the view: either axis may
# carry the discrimination.
_EITHER_AXIS_PREFIXES = (
    "comb.and",
    "comb.or",
    "comb.not",
    "comb.nested",
    "comb.mixed",
    "view.",
)


def _required_axis(name):
    """The axis `name` must discriminate on, or None if either will do."""
    if name.startswith(_EDGE_AXIS_PREFIXES):
        return "edges"
    if name.startswith(_NODE_AXIS_PREFIXES):
        return "nodes"
    return None


def _assert_discriminates(side_name, label, probed, baseline, name):
    """The filter kept some entities and dropped some, on the axis it targets."""
    axes = _discriminating_axes(probed, baseline)
    assert axes, (
        f"{side_name} {label} neither kept nor dropped anything: it selected "
        f"everything or nothing, so its parity case is vacuous"
    )
    required = _required_axis(name)
    if required is not None and required in probed:
        assert required in axes, (
            f"{side_name} {label} left the {required} axis empty (or untouched) "
            f"even though the expression predicates over {required}: it matched "
            f"no {required}, so its parity case is vacuous"
        )


def test_every_expr_declares_an_axis():
    """Every expression name falls in a known family, so none defaults silently.

    `_required_axis` reads the name. A typo, or a new family added without a
    prefix, would silently return None and relax the guard to "either axis" —
    which is exactly the loophole the guard exists to close.
    """
    known = _EDGE_AXIS_PREFIXES + _NODE_AXIS_PREFIXES + _EITHER_AXIS_PREFIXES
    unclassified = [name for name in EXPRS if not name.startswith(known)]
    assert unclassified == [], (
        f"expressions with no declared axis family: {unclassified} — add the "
        f"prefix to _EDGE_AXIS_PREFIXES / _NODE_AXIS_PREFIXES / "
        f"_EITHER_AXIS_PREFIXES so the non-vacuity guard knows what to require"
    )


# Unfiltered probe values, memoized per side: the matrix asks for them once per
# expression and each costs a round trip on the remote.
_BASELINES = {}


def _baseline(pair, key, probe, reach):
    if key not in _BASELINES:
        _BASELINES[key] = {
            "local": canonical(probe(reach(pair.local))),
            "remote": canonical(probe(reach(pair.remote))),
        }
    return _BASELINES[key]


# Expressions that select *everything* on this graph, so they cannot
# discriminate — and are asserted to do exactly that by
# `test_universal_expr_selects_everything` rather than being skipped.
#
# The first four are true of every entity in an EVENT graph: deletions are
# recorded as tombstones but never remove events, so "valid", "active at some
# point" and "the latest snapshot" all cover the whole graph. Their
# discriminating counterparts *are* in the matrix, scoped to a window
# (`scoped.edge.is_valid`, `scoped.node.is_active`) or negated
# (`pred.edge.is_deleted`) — those prove the validity and activity axes really
# do cross the wire.
#
# The last three are a property of the combinator lowering: a disjunction or a
# negation across two *different* scopes cannot exclude anything, because each
# branch leaves the other scope unconstrained.
_UNIVERSAL_EXPRS = {
    "universal.node.is_active": (
        lambda: f.Node.is_active(),
        "every node in an EVENT graph has an event in the unwindowed view",
    ),
    "universal.edge.is_valid": (
        lambda: f.Edge.is_valid(),
        "an EVENT graph keeps deleted events, so every edge is valid overall",
    ),
    "universal.exploded.is_valid": (
        lambda: f.ExplodedEdge.is_valid(),
        "as above, per exploded event rather than per edge",
    ),
    "universal.view.snapshot_latest": (
        lambda: f.Graph.snapshot_latest(),
        "the latest snapshot of an EVENT graph is the whole graph",
    ),
    "universal.mixed_or": (
        lambda: (f.Node.name() == "iso") | (f.Edge.property("weight") > 3.0),
        "a node OR an edge predicate: each branch leaves the other entity "
        "type unconstrained, so the disjunction admits everything",
    ),
    "universal.view_or": (
        lambda: f.Graph.at(3) | f.Graph.at(5),
        "a disjunction of two view scopes widens rather than narrows",
    ),
    "universal.view_not": (
        lambda: ~f.Graph.layer("knows"),
        "negating a view scope does not exclude entities from the result",
    ),
}


@pytest.mark.parametrize("name", sorted(EXPRS), ids=sorted(EXPRS))
def test_expr_discriminates(filter_pair, name):
    """The expression must keep some entities and drop some — on each side.

    Asserted per side, not across sides: two backends that both ignored an
    expression would agree with each other, so `test_expr_parity` alone proves
    nothing. This is what rules out a silently dropped filter.
    """
    build = EXPRS[name]
    baseline = _baseline(filter_pair, "graph", _probe_graph, lambda g: g)

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        probed = canonical(_probe_graph(side.filter(build())))
        _assert_discriminates(
            side_name, f"filter({name})", probed, baseline[side_name], name
        )


@pytest.mark.parametrize("name", sorted(_UNIVERSAL_EXPRS), ids=sorted(_UNIVERSAL_EXPRS))
def test_universal_expr_selects_everything(filter_pair, name):
    """Expressions that cannot discriminate here select *everything*, on both.

    These are the cases excluded from `test_expr_discriminates`. Rather than
    dropping them, they are pinned to the behaviour that makes them
    non-discriminating: they must return the unfiltered graph, identically on
    both sides. If either side ever starts excluding something, this fails and
    the expression moves into the discriminating matrix.
    """
    build, reason = _UNIVERSAL_EXPRS[name]
    assert_parity(filter_pair, lambda g: _probe_graph(g.filter(build())))

    baseline = _baseline(filter_pair, "graph", _probe_graph, lambda g: g)
    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        probed = canonical(_probe_graph(side.filter(build())))
        assert probed == baseline[side_name], (
            f"{side_name} filter({name}) is no longer universal ({reason}); "
            f"move it into EXPRS so it is held to the discriminating guard"
        )


# --- the three property sources ---------------------------------------------


def _names(h):
    return sorted(n.name for n in h.nodes)


def test_property_sources_are_distinct(filter_pair):
    """Metadata, aggregated property and temporal property are separate reads.

    `level` is metadata (`gold`/`silver`) *and* a temporal property (`bronze`)
    on overlapping but different nodes. Three filters over the same key must
    therefore give three different answers, and each must agree across the
    wire. In particular `property("level") == "gold"` selects *nothing*: the
    metadata value must not be visible to a property read. That is the check a
    lowering which collapses the two sources cannot pass.
    """
    metadata = lambda g: _names(g.filter(f.Node.metadata("level") == "gold"))
    aggregated = lambda g: _names(g.filter(f.Node.property("level") == "bronze"))
    crossed = lambda g: _names(g.filter(f.Node.property("level") == "gold"))
    temporal = lambda g: _names(
        g.filter(f.Node.property("level").temporal().any() == "bronze")
    )

    for read in (metadata, aggregated, crossed, temporal):
        assert_parity(filter_pair, read)

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        assert metadata(side) != aggregated(side), (
            f"{side_name}: the metadata and property reads of `level` returned "
            f"the same nodes — the two sources are not being distinguished"
        )
        assert crossed(side) == [], (
            f"{side_name}: property('level') == 'gold' matched "
            f"{crossed(side)}, but 'gold' is only a *metadata* value — the "
            f"property read is leaking metadata"
        )
        assert temporal(side) == aggregated(side), (
            f"{side_name}: the temporal and aggregated reads of `level` "
            f"disagree ({temporal(side)} vs {aggregated(side)})"
        )


def _edge_keys(h):
    return sorted(_edge_key(e) for e in h.edges)


def test_unqualified_edge_metadata_equality_matches_nothing_on_both_sides(filter_pair):
    """Edge metadata is layer-keyed, so an unlayered `==` matches no edge.

    `kind` is set on three edges, and `metadata("kind").is_some()` finds all
    three — but comparing it to a value without naming a layer matches nothing,
    while the same comparison under `Edge.layer("knows")` matches. That is an
    engine property rather than a wire problem, and this pins it as such: all
    three readings must agree across the wire, so the lowering is neither
    hiding nor inventing the layer qualification.
    """
    unqualified = lambda g: _edge_keys(g.filter(f.Edge.metadata("kind") == "strong"))
    present = lambda g: _edge_keys(g.filter(f.Edge.metadata("kind").is_some()))
    layered = lambda g: _edge_keys(
        g.filter(f.Edge.layer("knows").metadata("kind") == "strong")
    )

    for read in (unqualified, present, layered):
        assert_parity(filter_pair, read)

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        assert unqualified(side) == [], (
            f"{side_name}: unlayered edge metadata equality now matches "
            f"{unqualified(side)} — move it into the discriminating matrix"
        )
        assert present(side), f"{side_name}: no edge reports `kind` metadata at all"
        assert layered(side), (
            f"{side_name}: layer-qualified edge metadata equality matched "
            f"nothing, so edge metadata is unreachable by equality entirely"
        )


# --- application sites ------------------------------------------------------

# site -> (how to reach the handle, how to apply an expression, probe)
FILTER_SITES = {
    "graph": (lambda g: g, lambda g, e: g.filter(e), _probe_graph),
    "nodes": (lambda g: g.nodes, lambda g, e: g.nodes.filter(e), _probe_nodes),
    "node": (
        lambda g: g.node("hub"),
        lambda g, e: g.node("hub").filter(e),
        _probe_node,
    ),
    "path_from_node": (
        lambda g: g.node("hub").neighbours,
        lambda g, e: g.node("hub").neighbours.filter(e),
        _probe_path_from_node,
    ),
    "path_from_graph": (
        lambda g: g.nodes.neighbours,
        lambda g, e: g.nodes.neighbours.filter(e),
        _probe_path_from_graph,
    ),
}

# Expressions applied at every filter site: one per family, each chosen to be
# discriminating on *every* site (including `path_from_node`, whose members are
# only `hub`'s neighbours).
SITE_EXPRS = [
    "node.prop.gt",
    "node.name.contains",
    "node.type.eq",
    "degree.le",
    "source.node.metadata.eq",
    "source.node.temporal.first",
    "edge.prop.gt",
    "edge.src.name.eq",
    "comb.and",
    "comb.not",
    "comb.nested_3",
    "comb.mixed_and",
    "view.window",
    "view.layer",
    "view.chain_window_layers",
    "view.and_node",
    "scoped.node.window",
    "scoped.edge.layer",
    "exploded.prop.gt",
]

_SITE_MATRIX = [(site, name) for site in sorted(FILTER_SITES) for name in SITE_EXPRS]
_SITE_IDS = [f"{site}-{name}" for site, name in _SITE_MATRIX]


@pytest.mark.parametrize("site,name", _SITE_MATRIX, ids=_SITE_IDS)
def test_site_matrix_parity(filter_pair, site, name):
    """The same expression, at every handle that accepts one, agrees."""
    _, apply, probe = FILTER_SITES[site]
    build = EXPRS[name]
    assert_parity(filter_pair, lambda g: probe(apply(g, build())))


@pytest.mark.parametrize("site,name", _SITE_MATRIX, ids=_SITE_IDS)
def test_site_matrix_discriminates(filter_pair, site, name):
    """Each (site, expression) pair changes what that handle reports, per side."""
    reach, apply, probe = FILTER_SITES[site]
    build = EXPRS[name]
    baseline = _baseline(filter_pair, site, probe, reach)

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        probed = canonical(probe(apply(side, build())))
        _assert_discriminates(
            side_name, f"{site}.filter({name})", probed, baseline[side_name], name
        )


# --- the `collection[expr]` sugar -------------------------------------------
#
# `nodes[expr]` / `edges[expr]` narrow membership at that step only. Remotely
# they lower to `select(...)`, which is typed to the collection's own entity:
# `RemoteNodes.select` takes a node filter, `RemoteEdges.select` an edge filter.
# The expressions below are the ones both sides accept; the ones they disagree
# about are ledgered at the bottom of this module.

GETITEM_SITES = {
    "nodes": (
        lambda g: g.nodes,
        lambda g, e: g.nodes[e],
        _probe_nodes,
        [
            "node.prop.gt",
            "node.name.contains",
            "node.type.is_in",
            "node.id.starts_with",
            "node.name.fuzzy",
            "degree.le",
            "source.node.metadata.eq",
            "source.node.temporal.first",
            "comb.and",
            "comb.not",
            "comb.nested_3",
        ],
    ),
    "edges": (
        lambda g: g.edges,
        lambda g, e: g.edges[e],
        _probe_edges,
        [
            "edge.prop.gt",
            "edge.prop.is_none",
            "edge.prop.starts_with",
            "edge.src.name.eq",
            "edge.dst.type.eq",
            "edge.src.property.gt",
            "edge.src.name.fuzzy",
            "source.edge.metadata.is_some",
            "comb.edge_or",
            "comb.edge_not",
            "exploded.prop.gt",
            "exploded.metadata.eq",
        ],
    ),
}

_GETITEM_MATRIX = [
    (site, name)
    for site, (_, _, _, names) in sorted(GETITEM_SITES.items())
    for name in names
]
_GETITEM_IDS = [f"{site}-{name}" for site, name in _GETITEM_MATRIX]


@pytest.mark.parametrize("site,name", _GETITEM_MATRIX, ids=_GETITEM_IDS)
def test_getitem_parity(filter_pair, site, name):
    """`collection[expr]` selects the same members locally and remotely."""
    _, apply, probe, _ = GETITEM_SITES[site]
    build = EXPRS[name]
    assert_parity(filter_pair, lambda g: probe(apply(g, build())))


@pytest.mark.parametrize("site,name", _GETITEM_MATRIX, ids=_GETITEM_IDS)
def test_getitem_discriminates(filter_pair, site, name):
    """`collection[expr]` keeps some members and drops some, on each side."""
    reach, apply, probe, _ = GETITEM_SITES[site]
    build = EXPRS[name]
    baseline = _baseline(filter_pair, f"getitem.{site}", probe, reach)

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        probed = canonical(probe(apply(side, build())))
        _assert_discriminates(
            side_name, f"{site}[{name}]", probed, baseline[side_name], name
        )


def test_getitem_narrows_membership_where_filter_rescopes(filter_pair):
    """`nodes[expr]` drops members; `nodes.filter(expr)` rescopes them.

    The two are not synonyms, and both sides must draw the line in the same
    place — otherwise one of them is quietly the other, and a caller reading
    `len()` gets a different answer over the wire. Same expression, both forms.
    """
    build = lambda: f.Node.property("score") > 15
    assert_parity(filter_pair, lambda g: sorted(n.name for n in g.nodes[build()]))
    assert_parity(
        filter_pair, lambda g: sorted(n.name for n in g.nodes.filter(build()))
    )

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        selected = sorted(n.name for n in side.nodes[build()])
        filtered = sorted(n.name for n in side.nodes.filter(build()))
        assert selected != filtered, (
            f"{side_name}: nodes[expr] and nodes.filter(expr) returned the same "
            f"members ({selected}) — one of the two forms is not doing its job"
        )


# --- error parity -----------------------------------------------------------

# Expressions that build fine but must be *rejected* when applied: the value
# does not match the property's type, or the operator does not apply to it.
# Both backends have to refuse them, with the same exception type and the same
# message — a one-sided acceptance would mean the wire format is more (or less)
# permissive than the engine.
REJECTED_EXPRS = {
    "reject.prop_int_vs_str": lambda: f.Node.property("score") > "banana",
    "reject.prop_float_vs_str": lambda: f.Edge.property("weight") == "heavy",
    "reject.starts_with_on_int": lambda: f.Node.property("score").starts_with("1"),
    "reject.contains_on_int": lambda: f.Node.property("score").contains("1"),
    "reject.ends_with_on_float": lambda: f.Edge.property("weight").ends_with("5"),
    "reject.metadata_str_vs_int": lambda: f.Node.metadata("level") > 5,
    "reject.unknown_property": lambda: f.Node.property("nope") > 1,
    "reject.unknown_metadata": lambda: f.Node.metadata("nope") > 1,
    "reject.degree_vs_str": lambda: f.Node.degree() > "x",
    # `avg` is F64 and `len` is U64, so neither accepts a plain Python int here.
    "reject.avg_of_int_property": (
        lambda: f.Node.property("score").temporal().avg() > 20
    ),
    "reject.len_of_int_property": (
        lambda: f.Node.property("score").temporal().len() > 1
    ),
}


@pytest.mark.parametrize("name", sorted(REJECTED_EXPRS), ids=sorted(REJECTED_EXPRS))
def test_rejected_expr_parity(filter_pair, name):
    """Both sides reject the expression, with the same type and the same reason.

    `assert_parity` already fails if only one side raises, or if the exception
    types differ. On top of that the local message must appear verbatim in the
    remote one: the server's diagnostic is wrapped in a GraphQL error envelope,
    but the *reason* has to survive, or a caller cannot act on it.
    """
    build = REJECTED_EXPRS[name]
    read = lambda g: sorted(n.name for n in g.filter(build()).nodes)
    assert_parity(filter_pair, read)

    with pytest.raises(Exception) as local_exc:
        read(filter_pair.local)
    with pytest.raises(Exception) as remote_exc:
        read(filter_pair.remote)
    assert str(local_exc.value) in str(remote_exc.value), (
        f"{name}: the remote rejection lost the local reason\n"
        f"  local : {local_exc.value}\n"
        f"  remote: {remote_exc.value}"
    )


@pytest.mark.parametrize("name", sorted(REJECTED_EXPRS), ids=sorted(REJECTED_EXPRS))
def test_rejected_expr_parity_at_nodes_filter(filter_pair, name):
    """The same rejections happen at `nodes.filter`, not only at `graph.filter`."""
    build = REJECTED_EXPRS[name]
    assert_parity(
        filter_pair, lambda g: sorted(n.name for n in g.nodes.filter(build()))
    )


# Node collections that take a `[expr]` subscript. Each must refuse an
# edge-testing expression identically, so the check runs at every site rather
# than only at `graph.nodes`.
NODE_SUBSCRIPT_SITES = {
    "nodes": lambda g: g.nodes,
    "path_from_node": lambda g: g.node("hub").neighbours,
    "path_from_graph": lambda g: g.nodes.neighbours,
}


@pytest.mark.parametrize(
    "site", sorted(NODE_SUBSCRIPT_SITES), ids=sorted(NODE_SUBSCRIPT_SITES)
)
@pytest.mark.parametrize(
    "kind,build",
    [
        ("edge", lambda: f.Edge.property("weight") > 2.0),
        ("exploded", lambda: f.ExplodedEdge.property("weight") > 2.0),
    ],
    ids=["edge", "exploded"],
)
def test_edge_expr_in_a_node_subscript_is_refused_the_same_way(
    filter_pair, site, kind, build
):
    """An edge test in `nodes[expr]` raises the same exception on both sides.

    An edge predicate — aggregated or exploded — says nothing about which nodes
    belong in a node collection, so both backends refuse it — and they have to
    refuse it as the *same* exception with the same reason, or a caller cannot
    write one `except` clause that works against either graph.
    """
    reach = NODE_SUBSCRIPT_SITES[site]
    read = lambda g: [n.name for n in reach(g)[build()]]
    assert_parity(filter_pair, read)

    with pytest.raises(Exception) as local_exc:
        read(filter_pair.local)
    with pytest.raises(Exception) as remote_exc:
        read(filter_pair.remote)
    assert str(local_exc.value) in str(remote_exc.value), (
        f"{site}[{kind} expr]: the remote rejection lost the local reason\n"
        f"  local : {local_exc.value}\n"
        f"  remote: {remote_exc.value}"
    )


def test_is_in_with_a_mistyped_value_matches_nothing_on_both_sides(filter_pair):
    """`is_in` with values of the wrong type is empty, not an error.

    Unlike `>` against a mistyped value — which both sides reject — a mistyped
    `is_in` list is accepted and simply matches no node. That asymmetry is
    surprising enough to pin, and it has to be the *same* surprise on both
    sides, since a caller cannot tell "no matches" from "bad query" otherwise.
    """
    build = lambda: f.Node.property("score").is_in(["not", "numbers"])
    assert_parity(
        filter_pair, lambda g: sorted(n.name for n in g.filter(build()).nodes)
    )

    for side_name, side in (
        ("local", filter_pair.local),
        ("remote", filter_pair.remote),
    ):
        assert [n.name for n in side.filter(build()).nodes] == [], (
            f"{side_name}: a mistyped is_in matched nodes; if this now raises "
            f"or filters, move the case into REJECTED_EXPRS"
        )


# --- divergence ledger ------------------------------------------------------

# Local↔remote filter gaps found by this module. Each is replayed below as a
# strict xfail, so the day it closes the suite goes red and the entry has to be
# deleted here and in `_parity.py`.
FILTER_GAP_CASES = [
    # ExplodedEdge expressions — predicates AND property/metadata reads — now
    # cross the wire; they are in the matrix above (`EXPLODED_EXPRS`).
    # Remote-only application sites: the local Edge / Edges / NestedEdges have
    # no `filter` at all (locally, filtering is a node-view-op plus GraphView).
    (
        "filter.edges.filter",
        lambda g: sorted(
            (e.src.name, e.dst.name)
            for e in g.edges.filter(f.Edge.property("weight") > 2.0)
        ),
    ),
    (
        "filter.edge.filter",
        lambda g: g.edge("hub", "spoke2")
        .filter(f.Edge.property("weight") > 2.0)
        .src.name,
    ),
    (
        "filter.nested_edges.filter",
        lambda g: sorted(
            sorted((e.src.name, e.dst.name) for e in sub)
            for sub in g.nodes.edges.filter(f.Edge.property("weight") > 2.0)
        ),
    ),
]

_GAP_IDS = [f"{key}-{i}" for i, (key, _) in enumerate(FILTER_GAP_CASES)]


@pytest.mark.parametrize(
    "key,fn",
    [
        pytest.param(
            key,
            fn,
            marks=pytest.mark.xfail(reason=KNOWN_GAPS[key], strict=True),
            id=case_id,
        )
        for (key, fn), case_id in zip(FILTER_GAP_CASES, _GAP_IDS)
    ],
)
def test_filter_known_gap(filter_pair, key, fn):
    assert_parity(filter_pair, fn)


# `[expr]` with general (non-kind-typed) expressions: select on the wire now
# takes GqlFilter, so graph-view / node / mixed expressions narrow membership
# the same way local core select does.
SUBSCRIPT_GENERAL_EXPRS = [
    (
        "nodes[view]",
        "nodes",
        # window [2,5) — keeps the spokes and hub, drops leaf (t=5) and iso (t=6)
        lambda g: sorted(n.name for n in g.nodes[f.Graph.window(2, 5)]),
    ),
    (
        "nodes[view & node]",
        "nodes",
        lambda g: sorted(
            n.name
            for n in g.nodes[f.Graph.window(2, 7) & (f.Node.property("score") > 15)]
        ),
    ),
    (
        "edges[node]",
        "edges",
        lambda g: sorted(
            (e.src.name, e.dst.name) for e in g.edges[f.Node.property("score") > 15]
        ),
    ),
    (
        "edges[view]",
        "edges",
        lambda g: sorted(
            (e.src.name, e.dst.name) for e in g.edges[f.Graph.layer("knows")]
        ),
    ),
    (
        "edges[mixed]",
        "edges",
        lambda g: sorted(
            (e.src.name, e.dst.name)
            for e in g.edges[
                (f.Node.property("score") > 5) & (f.Edge.property("weight") > 2.0)
            ]
        ),
    ),
]


def _unfiltered_members(g, kind):
    if kind == "nodes":
        return sorted(n.name for n in g.nodes)
    return sorted((e.src.name, e.dst.name) for e in g.edges)


@pytest.mark.parametrize(
    "name,kind,fn", SUBSCRIPT_GENERAL_EXPRS, ids=[c[0] for c in SUBSCRIPT_GENERAL_EXPRS]
)
def test_subscript_general_expr_parity(filter_pair, name, kind, fn):
    assert_parity(filter_pair, fn)


@pytest.mark.parametrize(
    "name,kind,fn", SUBSCRIPT_GENERAL_EXPRS, ids=[c[0] for c in SUBSCRIPT_GENERAL_EXPRS]
)
def test_subscript_general_expr_discriminates(filter_pair, name, kind, fn):
    """The subscript must narrow membership on each side — a select that
    silently keeps everything (or drops everything) must fail here."""
    for side_name, g in (("local", filter_pair.local), ("remote", filter_pair.remote)):
        selected = fn(g)
        everyone = _unfiltered_members(g, kind)
        assert selected, f"{side_name}: {name} selected nothing"
        assert selected != everyone, f"{side_name}: {name} did not narrow membership"


# Filter forms that exist in `raphtory.filter` but that no parity case can
# reach, with the reason. Recorded rather than omitted, so the ledger stays the
# complete picture of what this module does *not* cover.
UNREACHABLE_FILTER_FORMS = ["filter.node.by_state_column"]


def test_filter_gap_cases_are_all_ledgered():
    """Every gap this module knows about corresponds to a KNOWN_GAPS entry."""
    for key, _ in FILTER_GAP_CASES:
        assert key in KNOWN_GAPS, f"gap case {key!r} missing from KNOWN_GAPS ledger"
    for key in UNREACHABLE_FILTER_FORMS:
        assert key in KNOWN_GAPS, f"unreachable form {key!r} missing from KNOWN_GAPS"


def test_by_state_column_needs_a_boolean_state_column():
    """`Node.by_state_column` cannot be built from the shared surface.

    It wants a *boolean* column of an `OutputNodeState`, and no algorithm on
    the drop-in surface produces one, so the expression cannot even be
    constructed — there is nothing to apply to either graph. Pinned as an error
    rather than left as an untested corner, and ledgered as
    `filter.node.by_state_column`.
    """
    from raphtory import Graph
    from raphtory.algorithms import pagerank

    g = Graph()
    g.add_node(1, "a")
    g.add_node(2, "b")
    g.add_edge(3, "a", "b")
    state = pagerank(g)

    with pytest.raises(ValueError):
        f.Node.by_state_column(state, "pagerank_score")
