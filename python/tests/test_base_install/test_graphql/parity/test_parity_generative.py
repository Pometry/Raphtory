"""Property-based parity: `local == remote` over *generated* inputs.

The enumerated parity suites fix a graph and enumerate calls; this module
inverts that — Hypothesis generates the graph, the filter expression, the view
chain — and the invariant stays the one the whole directory is built on: run
the same thing on a local ``Graph`` and a ``RemoteGraph`` and the answers must
agree (value parity), or both sides must refuse (exception parity). The
generator explores combinations no enumeration lists: interleaved writes and
deletes on colliding entities, filter trees mixing property, field and view
atoms under ``& | ~``, view chains with arbitrary bounds.

Determinism: every property runs with ``derandomize=True`` and
``database=None`` — the example sequence is derived from the test itself, so
CI runs are reproducible without a seed database, and a failure's minimal
example (a plain tuple tree, printed by Hypothesis) can be replayed verbatim.
``deadline=None`` because every example crosses a real HTTP server.

Server economics: one ``GraphServer`` serves the whole module. Mutating
properties create a *fresh remote graph per example* (unique name from a
module counter — graph creation is ~15ms, examples stay independent, and the
server holds hundreds of small graphs without strain); read-only properties
share one module-scoped pair; RPC-count properties share one counting proxy
and reset its counter per example.

``max_examples`` per property is budgeted to keep the whole module well
under two minutes: a fresh-graph example costs ~10-40ms (server calls
dominate), a shared-pair or RPC-count example ~1-3ms — measured, not guessed,
so the counts below (150-300 per property) leave several-fold headroom for
slower CI machines.
"""

import itertools
import tempfile

import pytest

hypothesis = pytest.importorskip("hypothesis")

from hypothesis import HealthCheck, event, example, given, settings, target
from hypothesis import strategies as st

from _parity import GraphPair, assert_parity, canonical
from _rpc import counting_remote_graph
from _strategies import (
    LAYERS,
    NODE_TYPES,
    apply_op,
    apply_ops,
    apply_view_chain,
    compile_filter,
    generated_case,
    leaf_kinds,
    safe_write_ops,
    view_chains,
    write_ops,
)
from raphtory import Graph
from raphtory.graphql import (
    GraphServer,
    RemoteEdgeAddition,
    RemoteNodeAddition,
    RemoteUpdate,
)


def _gen_settings(max_examples):
    return settings(
        max_examples=max_examples,
        deadline=None,  # every example crosses a real HTTP server
        derandomize=True,  # deterministic example sequence — see module docstring
        database=None,  # no cross-run state; derandomize makes it redundant
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    )


# --- fixtures -----------------------------------------------------------------

# Unique remote-graph name per example. A module counter (not time/random) so
# names never depend on anything outside Hypothesis' control.
_GRAPH_SEQ = itertools.count()


@pytest.fixture(scope="module")
def client():
    """One server (and client) for every generated example in this module."""
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            yield server.get_client()


def _fresh_pair(client, case):
    """A new local Graph and a new remote graph, seeded with ``ops`` in lockstep.

    Returns ``(pair, rejected)`` where ``rejected`` counts ops both sides
    refused (per-op exception parity is asserted inside ``apply_ops``).
    """
    local = Graph()
    name = f"gen_{next(_GRAPH_SEQ)}"
    client.new_graph(name, "EVENT")
    remote = client.remote_graph(name)
    rejected = apply_ops(local, remote, case)
    return GraphPair(local=local, remote=remote), rejected


def _build_view_fixture(g):
    """A fixed graph on which every generated view chain and filter can bite:
    events at t=0..12, all three pool layers plus the default, node types,
    typed properties, and a tombstone."""
    g.add_node(0, "n0", node_type="person", properties={"p_int": 1, "p_str": "red"})
    g.add_node(2, "n1", node_type="bot", properties={"p_int": 7})
    g.add_node(5, "n2", properties={"p_float": 2.5, "p_bool": True})
    g.add_edge(1, "n0", "n1", properties={"p_float": 0.5}, layer="alpha")
    g.add_edge(3, "n1", "n2", properties={"p_int": 42}, layer="beta")
    g.add_edge(4, "n0", "n2", properties={"p_str": "blue"}, layer="gamma")
    g.add_edge(6, "n2", "n0")  # default layer
    g.add_edge(8, "n0", "n1", properties={"p_float": 1.5}, layer="alpha")
    g.add_edge(10, "n3", "n4", layer="beta")
    g.delete_edge(11, "n0", "n1", layer="alpha")
    g.add_node(12, "n5", node_type="org")


@pytest.fixture(scope="module")
def view_pair(client):
    """A module-scoped fixed pair for read-only generated probes (P4)."""
    local = Graph()
    _build_view_fixture(local)
    client.new_graph("view_fixture", "EVENT")
    remote = client.remote_graph("view_fixture")
    _build_view_fixture(remote)
    return GraphPair(local=local, remote=remote)


@pytest.fixture(scope="module")
def counted():
    """A counting proxy for the RPC-count properties (P5), reused across
    examples with ``counter.reset()`` — an RPC *count* does not depend on
    graph content, so accumulated writes are harmless (and the generated
    writes are drawn from ``safe_write_ops``, which cannot be rejected)."""
    with counting_remote_graph(_build_view_fixture) as (remote, counter):
        yield remote, counter


# --- probes -------------------------------------------------------------------
#
# Structured like the enumerated suites' probes: dicts keyed by entity
# identity, leaf values flattened to strings, so `canonical` cannot sort two
# genuinely different answers into agreement (it reorders unkeyed sequences),
# and so ordered facts (timelines) stay order-sensitive inside one string.


def _norm_value(v):
    if isinstance(v, bool):
        return f"b:{v}"
    if isinstance(v, float):
        return f"f:{round(v, 9)}"
    if isinstance(v, int):
        return f"i:{v}"
    if isinstance(v, str):
        return f"s:{v!r}"
    if isinstance(v, dict):
        inner = ",".join(f"{k}={_norm_value(v[k])}" for k in sorted(v))
        return "{" + inner + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_norm_value(x) for x in v) + "]"
    return repr(v)


def _stamps(times):
    """A history/deletions sequence as one order-preserving string of
    ``(t, event_id)`` — the event_id half is what same-timestamp writes and
    explicit-id deletes are distinguished by."""
    return ";".join(f"{x.t}#{x.event_id}" for x in times)


def _timelines(props):
    """Every temporal property as an order-preserving ``t#event_id#value``
    string, keyed by property name."""
    temporal = props.temporal
    return {
        key: ";".join(
            f"{t.t}#{t.event_id}#{_norm_value(v)}" for t, v in temporal.get(key).items()
        )
        for key in temporal.keys()
    }


def _metadata(meta):
    return {k: _norm_value(v) for k, v in meta.items()}


def probe_full_state(g):
    """The full-state readback P1 compares: node names/types/histories,
    edge pairs+layers, property timelines, metadata, deletions."""
    nodes = {}
    for n in g.nodes:
        nodes[n.name] = {
            "type": n.node_type,
            "history": _stamps(n.history),
            "props": _timelines(n.properties),
            "meta": _metadata(n.metadata),
        }
    edges = {}
    for e in g.edges:
        edges[f"{e.src.name}->{e.dst.name}"] = {
            "layers": ",".join(sorted(e.layer_names)),
            "history": _stamps(e.history),
            "deletions": _stamps(e.deletions),
            "props": _timelines(e.properties),
            "meta": _metadata(e.metadata),
        }
    return {
        "nodes": nodes,
        "edges": edges,
        "graph_meta": _metadata(g.metadata),
        "n": g.count_nodes(),
        "m": g.count_edges(),
    }


def probe_membership(h):
    """The lighter probe filters and view chains are compared through:
    membership plus per-entity facts a rescope moves (degree, layers)."""
    return {
        "nodes": {n.name: n.degree() for n in h.nodes},
        "edges": {
            f"{e.src.name}->{e.dst.name}": ",".join(sorted(e.layer_names))
            for e in h.edges
        },
    }


def _run(fn, g):
    try:
        return fn(g), None
    except Exception as exc:  # noqa: BLE001 — parity check needs the type
        return None, exc


def _parity_with_outcome(pair, fn):
    """`assert_parity`, but reporting which branch it took: ``"raised"`` (both
    sides refused, same exception type) or ``"agreed"`` (canonical equality).
    The generative properties feed this into Hypothesis' event statistics."""
    local_result, local_exc = _run(fn, pair.local)
    remote_result, remote_exc = _run(fn, pair.remote)
    if local_exc is not None or remote_exc is not None:
        assert type(local_exc) is type(remote_exc), (
            f"exception parity mismatch: "
            f"local raised {local_exc!r}, remote raised {remote_exc!r}"
        )
        return "raised", None
    assert canonical(local_result) == canonical(
        remote_result
    ), f"value parity mismatch: local={local_result!r} remote={remote_result!r}"
    return "agreed", local_result


# --- P1: generated write sequences read back identically ------------------------


@given(case=write_ops())
# The empty graph is the shape most likely to break `earliest_time`,
# aggregations and `collect()`, and random sizing reaches it only ~0.6% of
# the time — so it is pinned as an explicit example rather than left to luck.
@example(case=({}, {}, []))
@_gen_settings(max_examples=150)
def test_generated_writes_full_state_parity(client, case):
    """Any generated write sequence leaves both sides in the same full state.

    Rejections along the way are themselves parity-checked per op (both sides
    must refuse, same type) and the sequence continues past them, so one
    conflicting metadata write cannot mask what the rest of the sequence does.
    """
    ops = case[2]
    pair, rejected = _fresh_pair(client, case)
    # These are Hypothesis `event` labels — they only shape the coverage
    # summary, never the assertions. Both are bucketed rather than exact so the
    # summary stays readable: an unbounded `rejected` count would print one
    # line per distinct number, so 3+ collapses into a single "3" bucket.
    event(f"writes: rejected={min(rejected, 3)}{'+' if rejected > 3 else ''}")
    event(
        f"writes: ops={'0' if not ops else '1-4' if len(ops) <= 4 else '5-8' if len(ops) <= 8 else '>8'}"
    )
    assert_parity(pair, probe_full_state)


# --- P2: generated filters on generated graphs ---------------------------------

# No fixed seed prefix: prepending one would mean every filter example runs
# against a graph that already contains those exact keys, layers and node
# types, so graphs *without* them could never be explored. `target` below
# steers generation toward expressions that match part of the graph — the same
# goal, without narrowing the input space.


def _classify_filtered(filtered, unfiltered):
    """What the filter actually did, named after the outcome.

    ``matched_some`` is the case worth generating: the filter kept part of the
    graph and dropped the rest, so local and remote agreeing on it is a real
    claim. The other two pass trivially — a backend that ignored filters
    entirely would still match all, and an empty result is the same `[]` on
    both sides either way.
    """
    if not filtered["nodes"] and not filtered["edges"]:
        return "matched_none"
    if filtered == unfiltered:
        return "matched_all"
    return "matched_some"


@given(case=generated_case(max_ops=16, with_expr=True))
@_gen_settings(max_examples=600)
def test_generated_filter_parity(client, case):
    """A generated expression over a generated graph selects the same thing
    through ``graph.filter`` on both sides — or is refused by both.

    The event statistics make vacuity visible instead of guessed: every
    example is classified by what the filter did — ``matched_some`` (kept
    part, dropped the rest — the case that proves anything), ``matched_all``,
    ``matched_none``, or ``rejected`` — and ``target`` steers generation
    toward ``matched_some``.
    """
    *_, expr = case
    pair, _ = _fresh_pair(client, case)
    outcome, filtered = _parity_with_outcome(
        pair, lambda g: probe_membership(g.filter(compile_filter(expr)))
    )
    if outcome == "raised":
        event("filter: rejected (both sides)")
        target(0.0, label="filters that matched some")
        return
    # Discrimination is classified on the local side only: value parity above
    # already forces the remote answer to be identical.
    kind = _classify_filtered(filtered, probe_membership(pair.local))
    event(f"filter: {kind}")
    target(1.0 if kind == "matched_some" else 0.0, label="filters that matched some")


# --- P3: generated expressions at the collection subscripts ---------------------


@given(case=generated_case(max_ops=16, with_expr=True))
@_gen_settings(max_examples=400)
def test_generated_subscript_parity(client, case):
    """``nodes[expr]`` and ``edges[expr]`` agree for generated expressions.

    A pure edge-testing expression says nothing about node membership, so on
    the node side both backends must *refuse* it — that expectation is
    asserted outright (not just as optional exception parity) whenever the
    generated tree's leaves are all edge-kind.
    """
    *_, expr = case
    pair, _ = _fresh_pair(client, case)
    kinds = leaf_kinds(expr)

    node_outcome, _ = _parity_with_outcome(
        pair, lambda g: sorted(n.name for n in g.nodes[compile_filter(expr)])
    )
    edge_outcome, _ = _parity_with_outcome(
        pair,
        lambda g: sorted(
            f"{e.src.name}->{e.dst.name}" for e in g.edges[compile_filter(expr)]
        ),
    )
    event(f"subscript: kinds={'+'.join(sorted(kinds))}")
    event(f"subscript: nodes[expr] {node_outcome}, edges[expr] {edge_outcome}")
    if kinds == {"edge"}:
        assert node_outcome == "raised", (
            f"nodes[{expr!r}] tests only edges, so both sides must refuse it — "
            f"instead both answered"
        )


# --- P4: generated view chains on a fixed graph ---------------------------------


def _probe_viewed(h):
    membership = probe_membership(h)
    start, end = h.start, h.end
    membership["bounds"] = f"{getattr(start, 't', start)}..{getattr(end, 't', end)}"
    return membership


@given(chain=view_chains())
@_gen_settings(max_examples=250)
def test_generated_view_chain_parity(view_pair, chain):
    """Any chain (length <= 3) of view ops with generated arguments installs
    the same view on both sides: same membership, same rescoped facts, same
    window bounds."""
    outcome, viewed = _parity_with_outcome(
        view_pair, lambda g: _probe_viewed(apply_view_chain(g, chain))
    )
    if outcome == "raised":
        event("view chain: rejected (both sides)")
        return
    baseline = _probe_viewed(view_pair.local)
    event(
        "view chain: narrowing"
        if viewed != baseline
        else "view chain: whole-graph view"
    )


# --- P5: transport invariants hold for generated inputs --------------------------

# Graph-handle terminals, each documented "Fires one RPC.".
_TERMINALS = {
    "count_nodes": lambda h: h.count_nodes(),
    "count_edges": lambda h: h.count_edges(),
    "earliest_time": lambda h: h.earliest_time,
    "unique_layers": lambda h: h.unique_layers,
}


@given(chain=view_chains(), terminal=st.sampled_from(sorted(_TERMINALS)))
@_gen_settings(max_examples=150)
def test_generated_chain_plus_terminal_is_one_rpc(counted, chain, terminal):
    """A generated view chain plus a single terminal crosses the wire exactly
    once — the chain itself contributes zero, whatever its shape."""
    remote, counter = counted
    counter.reset()
    _TERMINALS[terminal](apply_view_chain(remote, chain))
    assert counter.value == 1, (
        f"chain {chain!r} + {terminal}: expected exactly 1 RPC, "
        f"wire saw {counter.value}"
    )


@given(case=safe_write_ops())
@_gen_settings(max_examples=150)
def test_generated_write_is_one_rpc(counted, case):
    """Any generated write op is one round trip, whatever its arguments.

    Ops come from the never-rejected subset (graph-level, auto-creating,
    dtype-consistent pools) because the counting graph accumulates writes
    across examples and a rejection would abort the count mid-example.
    """
    remote, counter = counted
    counter.reset()
    schema, meta_schema, (op,) = case
    apply_op(remote, schema, meta_schema, op)
    assert (
        counter.value == 1
    ), f"write {op!r}: expected exactly 1 RPC, wire saw {counter.value}"


# --- P6: large-N collect pin (not generative) ------------------------------------

_BIG_NODES = 2000
_BIG_EDGES = 5000


def _build_big(g):
    """~2000 nodes / ~5000 distinct edges; batched on remote (the API built
    for this size), the equivalent loop locally — the same batch-vs-loop
    equivalence the enumerated write suite pins."""
    if hasattr(g, "add_nodes"):
        g.add_nodes(
            [
                RemoteNodeAddition(f"m{i}", updates=[RemoteUpdate(i % 50, {"s": 1.0})])
                for i in range(_BIG_NODES)
            ]
        )
        g.add_edges(
            [
                RemoteEdgeAddition(src, dst, updates=[RemoteUpdate(t)])
                for t, src, dst in _big_edges()
            ]
        )
        return
    for i in range(_BIG_NODES):
        g.add_node(i % 50, f"m{i}", properties={"s": 1.0})
    for t, src, dst in _big_edges():
        g.add_edge(t, src, dst)


def _big_edges():
    # Distinct (src, dst) pairs: within a pass srcs are unique, and each pass
    # uses a different dst offset, so no pair repeats and none is a self-loop.
    for i in range(_BIG_EDGES):
        yield i % 50, f"m{i % _BIG_NODES}", f"m{(i + i // _BIG_NODES + 1) % _BIG_NODES}"


def test_large_collect_is_one_rpc_and_equals_local():
    """One pinned size: collect() over ~2000 nodes / ~5000 edges is still
    exactly one request each, and the answer matches the local graph."""
    local = Graph()
    _build_big(local)
    with counting_remote_graph(_build_big) as (remote, counter):
        counter.reset()
        collected_nodes = remote.nodes.collect()
        assert counter.value == 1, f"nodes.collect() fired {counter.value} RPCs"
        assert len(collected_nodes) == local.count_nodes() == _BIG_NODES

        counter.reset()
        collected_edges = remote.edges.collect()
        assert counter.value == 1, f"edges.collect() fired {counter.value} RPCs"
        assert len(collected_edges) == local.count_edges() == _BIG_EDGES

        # Content equality with local: the full node-name list is a single
        # RPC; edges are spot-checked through the collected handles (each
        # handle read is its own RPC, so the full 5000x2 sweep is out of
        # budget — membership, counts and node identity carry the claim).
        counter.reset()
        remote_names = remote.nodes.name
        assert counter.value == 1, f"nodes.name fired {counter.value} RPCs"
        assert sorted(remote_names) == sorted(n.name for n in local.nodes)

        for e in collected_edges[:20]:
            assert local.has_edge(e.src.name, e.dst.name), (
                f"remote collected edge {e.src.name}->{e.dst.name} "
                f"does not exist locally"
            )
