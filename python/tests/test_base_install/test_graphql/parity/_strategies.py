"""Hypothesis strategies for the generative parity layer.

Everything here generates *data*, not raphtory objects: a write operation is a
tuple like ``("add_node", 3, "n1", {"q0": 7}, "person")``, a filter expression
is a nested tuple tree, a view chain is a list of op tuples. Data first,
objects second, for two reasons:

* Hypothesis shrinks and reports the generated value — a tuple tree makes the
  minimal failing example readable and replayable verbatim, where a
  ``FilterExpr``'s repr would be opaque;
* the same generated value is compiled/applied twice, once per side, so the
  two sides cannot diverge through object identity.

**Schema-first.** Each example first generates a *schema* — a mapping of
property names to types, the types themselves drawn from every leaf dtype
raphtory exposes plus recursively nested lists and maps (modelled on the
rust-side generator in ``db4-storage/src/pages/test_utils/props.rs``). Writes
then produce values conforming to the schema, and filter expressions draw
their keys *from* the schema with comparison values of the matching type. That
correlation is not seeding: no content is fixed — a given example's graph may
still contain any subset of its schema, including none of it — but an
expression no longer names a property that could not possibly exist, which
previously made a third of generated examples fail with "property does not
exist" before testing anything.

Name pools (nodes, layers, node types, property names) stay deliberately tiny
so that generated sequences actually collide: repeated updates to one entity,
same-timestamp writes, deletes of edges that exist, filters whose comparison
values really occur in the graph. Every schema key has one fixed type per
example, which keeps generated writes free of dtype conflicts (the enumerated
suite pins dtype-conflict rejection already).

Ops that raphtory legitimately rejects on *both* sides can still be generated
(metadata re-add with a new value, ``set_node_type`` on a typed node, and a
deliberate ~10% of filter leaves naming a key outside the schema);
``apply_ops`` and the filter properties treat "both sides reject identically"
as parity rather than avoiding the case.
"""

from __future__ import annotations

import datetime
from decimal import Decimal

from hypothesis import strategies as st

from raphtory import Prop
from raphtory import filter as f

# --- name pools -----------------------------------------------------------------

NODE_NAMES = ["n0", "n1", "n2", "n3", "n4", "n5"]
NODE_IDS_INT = [0, 1, 2, 3, 4, 5]
# A graph's id type is pinned by its first node write: string and integer ids
# cannot mix within one graph. Each example draws one flavour as part of its
# schema. "str" and "int" generate conforming graphs; "mixed" deliberately
# draws from both pools so the wrong-type-id rejection stays exercised as
# exception parity (both sides must refuse a mismatched id identically).
ID_FLAVOURS = {
    "str": NODE_NAMES,
    "int": NODE_IDS_INT,
    "mixed": NODE_NAMES[:3] + NODE_IDS_INT[:3],
}
LAYERS = ["alpha", "beta", "gamma"]
NODE_TYPES = ["person", "bot", "org"]
# Property-name vocabulary. Small and closed (like the pools above) so distinct
# examples reuse names and writes collide; which *type* a name has is decided
# per example by the generated schema.
PROP_NAMES = ["q0", "q1", "q2", "q3", "q4"]
META_NAMES = ["m0", "m1"]
# A key outside every schema, drawn by ~10% of property leaves on purpose so
# the "filter on an absent property" rejection stays exercised deliberately
# rather than dominating by accident.
MISSING_KEY = "q_missing"

_DT_A = datetime.datetime(2021, 3, 4, 5, 6, 7, tzinfo=datetime.timezone.utc)
_DT_B = datetime.datetime(2022, 11, 30, 23, 59, 59, tzinfo=datetime.timezone.utc)
_NDT_A = datetime.datetime(2021, 3, 4, 5, 6, 7)
_NDT_B = datetime.datetime(2022, 11, 30, 23, 59, 59)

# --- the type universe -----------------------------------------------------------
#
# One row per leaf dtype: the value pool (small, ordered-distinct, so
# comparisons collide *and* discriminate), the `Prop` constructor that pins the
# width on writes and comparison values (None = plain Python round-trips to the
# right dtype already), and the comparison kind (which operators apply).

_LEAF_TYPES = {
    # tag:       (pool,                              wrap,                kind)
    "i64": ([-(2**40), 0, 7, 42], None, "orderable"),
    "i32": ([-70000, 0, 3], Prop.i32, "orderable"),
    "u8": ([0, 7, 255], Prop.u8, "orderable"),
    "u16": ([0, 300, 65535], Prop.u16, "orderable"),
    "u32": ([0, 70000], Prop.u32, "orderable"),
    "u64": ([0, 2**40], Prop.u64, "orderable"),
    "f64": ([-3.25, 0.5, 1.5, 2.5], None, "orderable"),
    "f32": ([-3.25, 0.5, 1.5], Prop.f32, "orderable"),
    "bool": ([True, False], None, "flag"),
    "str": (["red", "green", "blue", "redish"], None, "text"),
    "dtime": ([_DT_A, _DT_B], Prop.aware_datetime, "orderable"),
    "ndtime": ([_NDT_A, _NDT_B], Prop.naive_datetime, "orderable"),
    "decimal": ([Decimal("3.14"), Decimal("-0.50")], Prop.decimal, "orderable"),
}

# Which comparison operators make sense per kind. This is THE registry: a leaf
# strategy draws an operator from its type's kind here, and `_apply_comparison`
# dispatches by the very same (dunder) name — adding an operator means adding
# it to exactly one row, and generation and application cannot drift apart.
# Args-arity is encoded in the entry: "value" ops take one comparison value of
# the leaf's type, "values" ops a small list of them, "bare" ops nothing.
KIND_OPERATORS = {
    "orderable": {
        "__eq__": "value",
        "__ne__": "value",
        "__lt__": "value",
        "__le__": "value",
        "__gt__": "value",
        "__ge__": "value",
        "is_in": "values",
        "is_some": "bare",
        "is_none": "bare",
    },
    "text": {
        "__eq__": "value",
        "__ne__": "value",
        "contains": "value",
        "starts_with": "value",
        "ends_with": "value",
        "is_in": "values",
        "is_some": "bare",
        "is_none": "bare",
    },
    "flag": {
        "__eq__": "value",
        "__ne__": "value",
        "is_some": "bare",
        "is_none": "bare",
    },
    # Containers (list/map values): equality and presence only.
    "container": {
        "__eq__": "value",
        "__ne__": "value",
        "is_some": "bare",
        "is_none": "bare",
    },
}


def _leaf_type_tags():
    return st.sampled_from(sorted(_LEAF_TYPES))


def prop_types():
    """A property *type*: a leaf dtype, or a list/map of a leaf dtype.

    Mirrors the rust generator's ``prop_type()`` (leaves + ``prop_recursive``);
    one level of nesting here, because every extra level multiplies the wire
    cost of each example while the encode/decode path it exercises is the same.
    """
    leaves = _leaf_type_tags().map(lambda t: ("leaf", t))
    return st.one_of(
        leaves,
        _leaf_type_tags().map(lambda t: ("list", t)),
        _leaf_type_tags().map(lambda t: ("map", t)),
    )


def prop_schemas(names=PROP_NAMES):
    """A schema: a subset of the property-name vocabulary, each name typed.

    ``min_size=1``: the schema is vocabulary, not content — property-free
    graphs are still generated (writes draw a subset of the schema, possibly
    none of it), but an *empty* schema would force every property leaf in
    ``filter_exprs`` onto the missing-key fallback, swamping the run with
    absent-property rejections.
    """
    return st.dictionaries(
        st.sampled_from(names), prop_types(), min_size=1, max_size=len(names)
    )


def _raw_value(type_):
    """A strategy for one *unwrapped* value of ``type_`` (plain Python data,
    kept raw in the generated tuples so shrunk examples stay readable)."""
    shape, tag = type_
    pool, _, _ = _LEAF_TYPES[tag]
    leaf = st.sampled_from(pool)
    if shape == "leaf":
        return leaf
    if shape == "list":
        return st.lists(leaf, min_size=1, max_size=3)
    return st.dictionaries(st.sampled_from(["x", "y", "z"]), leaf, min_size=1)


def wrap_value(type_, raw):
    """Pin ``raw`` to its schema type for a write or a comparison value.

    The wrap is what keeps a ``u8`` a ``u8`` across the write and the filter:
    without it a plain ``7`` would arrive as ``i64`` and either widen the
    property or mismatch the comparison.
    """
    shape, tag = type_
    _, wrap, _ = _LEAF_TYPES[tag]
    if shape == "leaf":
        return wrap(raw) if wrap else raw
    if shape == "list":
        return Prop.list([wrap(v) if wrap else v for v in raw])
    return Prop.map({k: wrap(v) if wrap else v for k, v in raw.items()})


def _kind(type_):
    shape, tag = type_
    if shape != "leaf":
        return "container"
    return _LEAF_TYPES[tag][2]


_times = st.integers(min_value=0, max_value=12)
# Explicit event ids sit above any auto-assigned id a short sequence produces
# (autos count writes from 0), so explicit and auto ids never collide.
_event_ids = st.sampled_from([None, 50, 51, 52])
_maybe_layer = st.sampled_from([None] + LAYERS)


@st.composite
def _props(draw, schema, max_size=3):
    """A property dict conforming to ``schema``: raw values, wrapped on apply."""
    if not schema:
        return {}
    keys = draw(
        st.lists(st.sampled_from(sorted(schema)), unique=True, max_size=max_size)
    )
    return {key: draw(_raw_value(schema[key])) for key in keys}


# --- write operations -------------------------------------------------------------
#
# Each op is a tuple whose first element names the call; `apply_op` dispatches.
# Ops carry raw values; the schema travels alongside (in the generated case)
# so `apply_op` can wrap each value to its pinned type at apply time.


def _op_strategies(schema, meta_schema, id_pool=NODE_NAMES):
    props = _props(schema)
    meta = _props(meta_schema, max_size=2)
    _names = st.sampled_from(id_pool)
    return dict(
        add_node=st.tuples(
            st.just("add_node"),
            _times,
            _names,
            props,
            st.sampled_from([None] + NODE_TYPES),
        ),
        add_edge=st.tuples(
            st.just("add_edge"), _times, _names, _names, props, _maybe_layer, _event_ids
        ),
        node_updates=st.tuples(
            st.just("node_updates"), _names, _times, props, _event_ids
        ),
        edge_updates=st.tuples(
            st.just("edge_updates"),
            _names,
            _names,
            _times,
            props,
            _maybe_layer,
            _event_ids,
        ),
        delete_edge=st.tuples(
            st.just("delete_edge"), _times, _names, _names, _maybe_layer, _event_ids
        ),
        node_metadata=st.tuples(st.just("node_metadata"), _names, meta),
        edge_metadata=st.tuples(
            st.just("edge_metadata"), _names, _names, meta, _maybe_layer
        ),
        graph_metadata=st.tuples(st.just("graph_metadata"), meta),
        set_node_type=st.tuples(
            st.just("set_node_type"), _names, st.sampled_from(NODE_TYPES)
        ),
    )


@st.composite
def generated_case(draw, max_ops=20, min_ops=0, with_expr=False):
    """A full generated example: ``(schema, ops)`` or ``(schema, ops, expr)``.

    The schema is drawn first; writes conform to it and (when requested) the
    filter expression draws its property keys from it — correlation by
    construction, not by seeding content.

    ``min_ops=0`` on purpose: the empty graph is a common edge case for
    ``earliest_time``, aggregations and ``collect()``. ``max_ops`` is generous
    because the interesting write bugs are ordering bugs, which need sequences
    long enough for the interleaving to happen.
    """
    schema = draw(prop_schemas())
    meta_schema = draw(prop_schemas(names=META_NAMES))
    id_pool = ID_FLAVOURS[draw(st.sampled_from(sorted(ID_FLAVOURS)))]
    ops = draw(
        st.lists(
            st.one_of(*(_op_strategies(schema, meta_schema, id_pool).values())),
            min_size=min_ops,
            max_size=max_ops,
        )
    )
    if not with_expr:
        return schema, meta_schema, ops
    expr = draw(filter_exprs(schema, id_pool=id_pool))
    return schema, meta_schema, ops, expr


def write_ops(min_size=0, max_size=20):
    """A ``(schema, meta_schema, ops)`` case — see ``generated_case``."""
    return generated_case(max_ops=max_size, min_ops=min_size)


def safe_write_ops():
    """Write ops that can never be rejected: graph-level, auto-creating, no
    metadata/node_type (whose write-once semantics can conflict with earlier
    examples). Used where a shared graph accumulates writes across examples
    (the RPC-count property) and an exception would abort the count.

    Values are drawn from a fixed all-leaf schema (one representative key per
    comparison kind) — the RPC-count property cares about call shapes, not
    type variety, and a fixed schema keeps every op self-contained."""
    schema = {
        "q0": ("leaf", "i64"),
        "q1": ("leaf", "str"),
        "q2": ("leaf", "f64"),
    }
    ops = _op_strategies(schema, {})
    _names = st.sampled_from(NODE_NAMES)
    return st.one_of(
        st.tuples(st.just("add_node"), _times, _names, _props(schema), st.none()),
        ops["add_edge"],
        ops["delete_edge"],
    ).map(lambda op: (schema, {}, [op]))


def _wrapped(schema, props):
    return {k: wrap_value(schema[k], v) for k, v in props.items()} or None


def apply_op(g, schema, meta_schema, op):
    """Apply one generated op to a graph handle (local or remote).

    Entity-scoped ops are guarded on existence: ``g.node(...)`` is ``None`` on
    both sides for an absent node, and the guard decision is a function of the
    op prefix alone, so both sides always skip (or apply) in lockstep.
    """
    tag = op[0]
    if tag == "add_node":
        _, t, name, props, node_type = op
        g.add_node(t, name, properties=_wrapped(schema, props), node_type=node_type)
    elif tag == "add_edge":
        _, t, src, dst, props, layer, event_id = op
        g.add_edge(
            t,
            src,
            dst,
            properties=_wrapped(schema, props),
            layer=layer,
            event_id=event_id,
        )
    elif tag == "node_updates":
        _, name, t, props, event_id = op
        node = g.node(name)
        if node is not None:
            node.add_updates(t, properties=_wrapped(schema, props), event_id=event_id)
    elif tag == "edge_updates":
        _, src, dst, t, props, layer, event_id = op
        edge = g.edge(src, dst)
        if edge is not None:
            edge.add_updates(
                t, properties=_wrapped(schema, props), layer=layer, event_id=event_id
            )
    elif tag == "delete_edge":
        _, t, src, dst, layer, event_id = op
        g.delete_edge(t, src, dst, layer=layer, event_id=event_id)
    elif tag == "node_metadata":
        _, name, meta = op
        node = g.node(name)
        if node is not None and meta:
            node.add_metadata(_wrapped(meta_schema, meta))
    elif tag == "edge_metadata":
        _, src, dst, meta, layer = op
        edge = g.edge(src, dst)
        if edge is not None and meta:
            edge.add_metadata(_wrapped(meta_schema, meta), layer=layer)
    elif tag == "graph_metadata":
        (_, meta) = op
        if meta:
            g.add_metadata(_wrapped(meta_schema, meta))
    elif tag == "set_node_type":
        _, name, node_type = op
        node = g.node(name)
        if node is not None:
            node.set_node_type(node_type)
    else:  # pragma: no cover — strategy and dispatch must stay in sync
        raise ValueError(f"unknown generated op {op!r}")


def apply_ops(local, remote, case):
    """Apply a generated case to both graphs with per-op exception parity.

    A rejected op (metadata conflict, node-type conflict) must be rejected by
    *both* sides with the same exception type; the sequence then continues, so
    one rejection does not shadow the rest of the generated sequence. Returns
    the number of rejected ops (for Hypothesis event statistics).
    """
    schema, meta_schema, ops = case[0], case[1], case[2]
    rejected = 0
    for op in ops:
        local_exc = remote_exc = None
        try:
            apply_op(local, schema, meta_schema, op)
        except Exception as exc:  # noqa: BLE001 — parity check needs the type
            local_exc = exc
        try:
            apply_op(remote, schema, meta_schema, op)
        except Exception as exc:  # noqa: BLE001 — parity check needs the type
            remote_exc = exc
        assert type(local_exc) is type(remote_exc), (
            f"write exception parity mismatch on {op!r}: "
            f"local raised {local_exc!r}, remote raised {remote_exc!r}"
        )
        if local_exc is not None:
            rejected += 1
    return rejected


# --- filter expressions -------------------------------------------------------


@st.composite
def _prop_leaf(draw, prefix, schema):
    """A property comparison leaf, keyed by the example's schema.

    The key is drawn from the schema (so the property *can* exist), the
    operator from the key's type kind, and the comparison value from the same
    type's pool — value-vs-property dtype mismatches cannot be generated. A
    deliberate ~10% of leaves use ``MISSING_KEY`` (typed i64) instead, keeping
    the absent-property rejection exercised on purpose.
    """
    keys = sorted(schema)
    # 1-in-20 per leaf; an expression holds several leaves, so the
    # per-expression rate lands near the intended ~10%.
    if not keys or draw(st.integers(0, 19)) == 0:
        key, type_ = MISSING_KEY, ("leaf", "i64")
    else:
        key = draw(st.sampled_from(keys))
        type_ = schema[key]
    ops = KIND_OPERATORS[_kind(type_)]
    op = draw(st.sampled_from(sorted(ops)))
    arity = ops[op]
    if arity == "bare":
        args = ()
    elif arity == "values":
        args = (draw(st.lists(_raw_value(type_), min_size=1, max_size=3)),)
    else:
        args = (draw(_raw_value(type_)),)
    return (prefix, key, type_, op, args)


_FIELD_OPS = ["__eq__", "__ne__", "contains", "starts_with", "ends_with", "is_in"]


@st.composite
def _field_leaf(draw, tag, values):
    op = draw(st.sampled_from(_FIELD_OPS))
    if op == "is_in":
        args = (
            draw(
                st.lists(st.sampled_from(values), min_size=1, max_size=3, unique=True)
            ),
        )
    else:
        args = (draw(st.sampled_from(values)),)
    return (tag, op, args)


def _view_atoms():
    window = st.tuples(_times, _times).map(lambda ab: ("gwindow", min(ab), max(ab) + 1))
    return st.one_of(
        window,
        st.tuples(st.just("glayer"), st.sampled_from(LAYERS)),
        st.tuples(st.just("gat"), _times),
        st.just(("glatest",)),
    )


def filter_exprs(schema, kinds=("node", "edge", "view"), id_pool=NODE_NAMES):
    """A recursive filter-expression tree, keyed by the example's schema.

    Leaves are weighted over combinators by ``st.recursive`` itself (it
    extends only a fraction of draws); depth is bounded by ``max_leaves=4``,
    which keeps combinator nesting at or below three levels.
    """
    # Name-field comparisons are string comparisons on both sides; an int
    # node's name is its decimal string, so the pools are stringified ids.
    names = [str(x) for x in id_pool]
    leaves = []
    if "node" in kinds:
        leaves += [
            _prop_leaf("nprop", schema),
            _field_leaf("nname", names),
            _field_leaf("ntype", NODE_TYPES),
        ]
    if "edge" in kinds:
        leaves += [
            _prop_leaf("eprop", schema),
            _field_leaf("esrc", names),
            _field_leaf("edst", names),
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


def _apply_comparison(target, op, args):
    # Operator names in the registry are the builder's own method names
    # (dunders included), so application is a single dispatch — the bare ops
    # simply carry an empty args tuple.
    return getattr(target, op)(*args)


def compile_filter(expr):
    """Compile a generated expression tree into a ``raphtory.filter`` object."""
    tag = expr[0]
    if tag == "and":
        return compile_filter(expr[1]) & compile_filter(expr[2])
    if tag == "or":
        return compile_filter(expr[1]) | compile_filter(expr[2])
    if tag == "not":
        return ~compile_filter(expr[1])
    if tag in ("nprop", "eprop"):
        _, key, type_, op, args = expr
        # Wrap comparison values to the schema type, exactly as writes do —
        # a u8 property is compared against a u8 value, not a widened i64.
        if args and KIND_OPERATORS[_kind(type_)][op] == "values":
            args = ([wrap_value(type_, v) for v in args[0]],)
        elif args:
            args = (wrap_value(type_, args[0]),)
        builder = f.Node.property(key) if tag == "nprop" else f.Edge.property(key)
        return _apply_comparison(builder, op, args)
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
