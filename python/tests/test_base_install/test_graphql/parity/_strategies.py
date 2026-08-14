"""Hypothesis strategies for the generative parity layer.

Everything here generates *data*, not raphtory objects: a write operation is a
tuple like ``("add_node", 3, "n1", {"p_int": 7}, "person")``, a filter
expression is a nested tuple tree, a view chain is a list of op tuples. Data
first, objects second, for two reasons:

* Hypothesis shrinks and reports the generated value — a tuple tree makes the
  minimal failing example readable and replayable verbatim, where a
  ``FilterExpr``'s repr would be opaque;
* the same generated value is compiled/applied twice, once per side, so the
  two sides cannot diverge through object identity.

Pools are deliberately tiny (6 node names, 3 layers, 6 property keys, ~4
values per key) so that generated sequences actually collide: repeated updates
to one entity, same-timestamp writes, deletes of edges that exist, filters
whose comparison values really occur in the graph. Every property key has a
*fixed* value type; that keeps generated writes free of dtype conflicts (the
enumerated suite pins dtype-conflict rejection already), so a generated
sequence explores state space instead of tripping over rejections.

Ops that raphtory legitimately rejects on *both* sides can still be generated
(metadata re-add with a new value, ``set_node_type`` on a typed node);
``apply_ops`` treats "both sides reject identically" as parity rather than
avoiding the case.
"""

from __future__ import annotations

import operator

from hypothesis import strategies as st

from raphtory import filter as f

# --- pools --------------------------------------------------------------------

NODE_NAMES = ["n0", "n1", "n2", "n3", "n4", "n5"]
LAYERS = ["alpha", "beta", "gamma"]
NODE_TYPES = ["person", "bot", "org"]

# One value type per key, values chosen so equality comparisons can bite.
PROP_POOLS = {
    "p_int": [0, 1, 7, 42],
    "p_float": [0.5, 1.5, 2.5, -3.25],
    "p_str": ["red", "green", "blue", "redish"],
    "p_bool": [True, False],
    "p_list": [[1, 2], [2, 3], [7]],
    "p_map": [{"x": 1, "y": 2}, {"x": 2, "y": 3}],
}
META_POOLS = {
    "m_int": [1, 2],
    "m_str": ["uk", "us"],
}

_times = st.integers(min_value=0, max_value=12)
# Explicit event ids sit above any auto-assigned id a short sequence produces
# (autos count writes from 0), so explicit and auto ids never collide.
_event_ids = st.sampled_from([None, 50, 51, 52])
_names = st.sampled_from(NODE_NAMES)
_maybe_layer = st.sampled_from([None] + LAYERS)


@st.composite
def _props(draw, pools=PROP_POOLS, max_size=3):
    keys = draw(
        st.lists(st.sampled_from(sorted(pools)), unique=True, max_size=max_size)
    )
    return {key: draw(st.sampled_from(pools[key])) for key in keys}


# --- write operations -----------------------------------------------------------

# Each op is a tuple whose first element names the call; `apply_op` dispatches.
_ADD_NODE = st.tuples(
    st.just("add_node"),
    _times,
    _names,
    _props(),
    st.sampled_from([None] + NODE_TYPES),
)
_ADD_EDGE = st.tuples(
    st.just("add_edge"), _times, _names, _names, _props(), _maybe_layer, _event_ids
)
_NODE_UPDATES = st.tuples(st.just("node_updates"), _names, _times, _props(), _event_ids)
_EDGE_UPDATES = st.tuples(
    st.just("edge_updates"), _names, _names, _times, _props(), _maybe_layer, _event_ids
)
_DELETE_EDGE = st.tuples(
    st.just("delete_edge"), _times, _names, _names, _maybe_layer, _event_ids
)
_NODE_METADATA = st.tuples(
    st.just("node_metadata"), _names, _props(pools=META_POOLS, max_size=2)
)
_EDGE_METADATA = st.tuples(
    st.just("edge_metadata"),
    _names,
    _names,
    _props(pools=META_POOLS, max_size=2),
    _maybe_layer,
)
_GRAPH_METADATA = st.tuples(
    st.just("graph_metadata"), _props(pools=META_POOLS, max_size=2)
)
_SET_NODE_TYPE = st.tuples(
    st.just("set_node_type"), _names, st.sampled_from(NODE_TYPES)
)


def write_ops(min_size=0, max_size=20):
    """A sequence of write operations over the shared pools.

    ``min_size=0`` on purpose: the empty graph is a common edge case for
    ``earliest_time``, aggregations and ``collect()``, and excluding it means
    the one shape most likely to break is the one shape never generated.

    ``max_size`` is generous because the interesting write bugs are ordering
    bugs — a later write landing in the wrong layer, or a metadata conflict
    only reachable after several interleavings — and those need sequences long
    enough for the interleaving to happen.
    """
    return st.lists(
        st.one_of(
            _ADD_NODE,
            _ADD_EDGE,
            _NODE_UPDATES,
            _EDGE_UPDATES,
            _DELETE_EDGE,
            _NODE_METADATA,
            _EDGE_METADATA,
            _GRAPH_METADATA,
            _SET_NODE_TYPE,
        ),
        min_size=min_size,
        max_size=max_size,
    )


def safe_write_ops():
    """Write ops that can never be rejected: graph-level, auto-creating, no
    metadata/node_type (whose write-once semantics can conflict with earlier
    examples). Used where a shared graph accumulates writes across examples
    (the RPC-count property) and an exception would abort the count."""
    return st.one_of(
        st.tuples(st.just("add_node"), _times, _names, _props(), st.none()),
        _ADD_EDGE,
        _DELETE_EDGE,
    )


def apply_op(g, op):
    """Apply one generated op to a graph handle (local or remote).

    Entity-scoped ops are guarded on existence: ``g.node(...)`` is ``None`` on
    both sides for an absent node, and the guard decision is a function of the
    op prefix alone, so both sides always skip (or apply) in lockstep.
    """
    tag = op[0]
    if tag == "add_node":
        _, t, name, props, node_type = op
        g.add_node(t, name, properties=props or None, node_type=node_type)
    elif tag == "add_edge":
        _, t, src, dst, props, layer, event_id = op
        g.add_edge(
            t, src, dst, properties=props or None, layer=layer, event_id=event_id
        )
    elif tag == "node_updates":
        _, name, t, props, event_id = op
        node = g.node(name)
        if node is not None:
            node.add_updates(t, properties=props or None, event_id=event_id)
    elif tag == "edge_updates":
        _, src, dst, t, props, layer, event_id = op
        edge = g.edge(src, dst)
        if edge is not None:
            edge.add_updates(
                t, properties=props or None, layer=layer, event_id=event_id
            )
    elif tag == "delete_edge":
        _, t, src, dst, layer, event_id = op
        g.delete_edge(t, src, dst, layer=layer, event_id=event_id)
    elif tag == "node_metadata":
        _, name, meta = op
        node = g.node(name)
        if node is not None and meta:
            node.add_metadata(meta)
    elif tag == "edge_metadata":
        _, src, dst, meta, layer = op
        edge = g.edge(src, dst)
        if edge is not None and meta:
            edge.add_metadata(meta, layer=layer)
    elif tag == "graph_metadata":
        (_, meta) = op
        if meta:
            g.add_metadata(meta)
    elif tag == "set_node_type":
        _, name, node_type = op
        node = g.node(name)
        if node is not None:
            node.set_node_type(node_type)
    else:  # pragma: no cover — strategy and dispatch must stay in sync
        raise ValueError(f"unknown generated op {op!r}")


def apply_ops(local, remote, ops):
    """Apply ``ops`` to both graphs with per-op exception parity.

    A rejected op (metadata conflict, node-type conflict) must be rejected by
    *both* sides with the same exception type; the sequence then continues, so
    one rejection does not shadow the rest of the generated sequence. Returns
    the number of rejected ops (for Hypothesis event statistics).
    """
    rejected = 0
    for op in ops:
        local_exc = remote_exc = None
        try:
            apply_op(local, op)
        except Exception as exc:  # noqa: BLE001 — parity check needs the type
            local_exc = exc
        try:
            apply_op(remote, op)
        except Exception as exc:  # noqa: BLE001 — parity check needs the type
            remote_exc = exc
        assert type(local_exc) is type(remote_exc), (
            f"write exception parity mismatch on {op!r}: "
            f"local raised {local_exc!r}, remote raised {remote_exc!r}"
        )
        if local_exc is not None:
            rejected += 1
    return rejected


# --- filter expressions -----------------------------------------------------------

# Property keys usable in comparisons (lists/maps stay write-only: ordering and
# string ops over them are not part of the filter surface).
_NUMERIC_KEYS = ["p_int", "p_float"]
_CMP_OPS = ["eq", "ne", "lt", "le", "gt", "ge"]
_STRING_OPS = ["eq", "ne", "contains", "starts_with", "ends_with", "is_in"]


@st.composite
def _node_prop_leaf(draw):
    key = draw(st.sampled_from(_NUMERIC_KEYS + ["p_str", "p_bool"]))
    if key == "p_bool":
        op = draw(st.sampled_from(["eq", "ne"]))
    elif key == "p_str":
        op = draw(st.sampled_from(_STRING_OPS + ["is_some", "is_none"]))
    else:
        op = draw(st.sampled_from(_CMP_OPS + ["is_in", "is_some", "is_none"]))
    value = draw(_leaf_value(key, op))
    return ("nprop", key, op, value)


@st.composite
def _edge_prop_leaf(draw):
    key = draw(st.sampled_from(_NUMERIC_KEYS + ["p_str"]))
    if key == "p_str":
        op = draw(st.sampled_from(_STRING_OPS + ["is_some", "is_none"]))
    else:
        op = draw(st.sampled_from(_CMP_OPS + ["is_in", "is_some", "is_none"]))
    value = draw(_leaf_value(key, op))
    return ("eprop", key, op, value)


def _leaf_value(key, op):
    pool = PROP_POOLS[key]
    if op in ("is_some", "is_none"):
        return st.none()
    if op == "is_in":
        return st.lists(st.sampled_from(pool), min_size=1, max_size=3, unique_by=repr)
    return st.sampled_from(pool)


@st.composite
def _field_leaf(draw, tag, values):
    op = draw(st.sampled_from(_STRING_OPS))
    if op == "is_in":
        value = draw(
            st.lists(st.sampled_from(values), min_size=1, max_size=3, unique=True)
        )
    else:
        value = draw(st.sampled_from(values))
    return (tag, op, value)


def _view_atoms():
    window = st.tuples(_times, _times).map(lambda ab: ("gwindow", min(ab), max(ab) + 1))
    return st.one_of(
        window,
        st.tuples(st.just("glayer"), st.sampled_from(LAYERS)),
        st.tuples(st.just("gat"), _times),
        st.just(("glatest",)),
    )


def filter_exprs(kinds=("node", "edge", "view")):
    """A recursive filter-expression tree over the shared pools.

    Leaves are weighted over combinators by ``st.recursive`` itself (it
    extends only a fraction of draws); depth is bounded by ``max_leaves=4``,
    which keeps combinator nesting at or below three levels.
    """
    leaves = []
    if "node" in kinds:
        leaves += [
            _node_prop_leaf(),
            _field_leaf("nname", NODE_NAMES),
            _field_leaf("ntype", NODE_TYPES),
        ]
    if "edge" in kinds:
        leaves += [
            _edge_prop_leaf(),
            _field_leaf("esrc", NODE_NAMES),
            _field_leaf("edst", NODE_NAMES),
        ]
    if "view" in kinds:
        leaves.append(_view_atoms())
    return st.recursive(
        st.one_of(leaves),
        lambda children: st.one_of(
            st.tuples(st.just("not"), children),
            st.tuples(st.just("and"), children, children),
            st.tuples(st.just("or"), children, children),
        ),
        max_leaves=4,
    )


def leaf_kinds(expr):
    """The set of leaf kinds ("node" / "edge" / "view") an expression tests."""
    tag = expr[0]
    if tag in ("and", "or"):
        return leaf_kinds(expr[1]) | leaf_kinds(expr[2])
    if tag == "not":
        return leaf_kinds(expr[1])
    if tag in ("nprop", "nname", "ntype"):
        return {"node"}
    if tag in ("eprop", "esrc", "edst"):
        return {"edge"}
    return {"view"}


def _apply_comparison(target, op, value):
    if op in ("eq", "ne", "lt", "le", "gt", "ge"):
        return getattr(operator, op)(target, value)
    if op == "is_some":
        return target.is_some()
    if op == "is_none":
        return target.is_none()
    return getattr(target, op)(value)  # contains / starts_with / ends_with / is_in


def compile_filter(expr):
    """Compile a generated expression tree into a ``raphtory.filter`` object."""
    tag = expr[0]
    if tag == "and":
        return compile_filter(expr[1]) & compile_filter(expr[2])
    if tag == "or":
        return compile_filter(expr[1]) | compile_filter(expr[2])
    if tag == "not":
        return ~compile_filter(expr[1])
    if tag == "nprop":
        return _apply_comparison(f.Node.property(expr[1]), expr[2], expr[3])
    if tag == "eprop":
        return _apply_comparison(f.Edge.property(expr[1]), expr[2], expr[3])
    if tag == "nname":
        return _apply_comparison(f.Node.name(), expr[1], expr[2])
    if tag == "ntype":
        return _apply_comparison(f.Node.node_type(), expr[1], expr[2])
    if tag == "esrc":
        return _apply_comparison(f.Edge.src().name(), expr[1], expr[2])
    if tag == "edst":
        return _apply_comparison(f.Edge.dst().name(), expr[1], expr[2])
    if tag == "gwindow":
        return f.Graph.window(expr[1], expr[2])
    if tag == "glayer":
        return f.Graph.layer(expr[1])
    if tag == "gat":
        return f.Graph.at(expr[1])
    if tag == "glatest":
        return f.Graph.latest()
    raise ValueError(f"unknown generated filter node {expr!r}")


# --- view chains ---------------------------------------------------------------


def view_chains(max_size=3):
    """A chain (length 1..max_size) of view ops with generated arguments."""
    window_args = st.tuples(_times, _times).map(lambda ab: (min(ab), max(ab) + 1))
    op = st.one_of(
        window_args.map(lambda ab: ("window", ab[0], ab[1])),
        st.tuples(st.just("at"), _times),
        st.tuples(st.just("snapshot_at"), _times),
        st.tuples(st.just("layer"), st.sampled_from(LAYERS + ["_default"])),
        st.tuples(
            st.just("layers"),
            st.lists(st.sampled_from(LAYERS), min_size=1, max_size=3, unique=True),
        ),
        st.tuples(st.just("shrink_start"), _times),
        st.tuples(st.just("shrink_end"), _times),
        window_args.map(lambda ab: ("shrink_window", ab[0], ab[1])),
    )
    return st.lists(op, min_size=1, max_size=max_size)


def apply_view_chain(h, chain):
    for op in chain:
        h = getattr(h, op[0])(*op[1:])
    return h
