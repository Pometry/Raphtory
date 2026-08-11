"""Local vs remote API parity: shared fixture, comparator, and divergence ledger.

The premise of RemoteGraph is that it is a drop-in replacement for the local
``Graph``. That gives differential tests a free oracle: a single ``build(g)``
seeds *both* graphs, a single call runs on *both*, and — once canonicalized —
their results must be equal. The local ``Graph`` is ground truth.

Three pieces live here:

* ``graph_pair`` — seed a local ``Graph`` and a ``RemoteGraph`` identically and
  yield both.
* ``canonical`` / ``assert_parity`` — normalize any result into a stable,
  comparable form (collection order, float precision, datetime tz) and assert
  local == remote, including exception parity.
* ``KNOWN_GAPS`` — the divergence ledger. APIs not yet at parity are recorded
  here (with a reason) rather than silently skipped, so the suite doubles as an
  executable parity spec that shrinks as gaps close.
"""

from __future__ import annotations

import contextlib
import datetime
import math
import tempfile
from dataclasses import dataclass

from raphtory import Graph
from raphtory.graphql import GraphServer


@dataclass
class GraphPair:
    """A local ``Graph`` and a ``RemoteGraph`` seeded with identical data."""

    local: object
    remote: object


@contextlib.contextmanager
def graph_pair(build, graph_type="EVENT"):
    """Seed a local ``Graph`` and a ``RemoteGraph`` with ``build``, yield both.

    ``build`` takes a single graph handle and applies writes using only the
    shared (drop-in) surface, so the exact same callable runs against each side.
    The server is started on enter and torn down on exit.
    """
    local = Graph()
    build(local)
    # TemporaryDirectory (outer) is torn down only after the server context
    # (inner) has stopped and flushed — so the dir outlives every write-back and
    # is then cleaned up, rather than leaked as `mkdtemp` would.
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            client = server.get_client()
            client.new_graph("g", graph_type)
            remote = client.remote_graph("g")
            build(remote)
            yield GraphPair(local=local, remote=remote)


# --- canonicalization -------------------------------------------------------

# Float comparison tolerance: serde round-trips can perturb the last bits.
_FLOAT_PLACES = 9


def canonical(value):
    """Normalize a raphtory result into a stable, comparable Python value.

    Absorbs the drift that would otherwise cause false diffs between an
    in-process result and one that crossed the wire: collection ordering, float
    precision, and datetime timezone. Entities are reduced to identity
    (node name, edge ``(src, dst)``) so the two sides compare structurally.
    """
    # bool is an int subclass — handle before int.
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, float):
        return "nan" if math.isnan(value) else round(value, _FLOAT_PLACES)
    if isinstance(value, (int, str)):
        return value
    if isinstance(value, datetime.datetime):
        # Compare as a tz-normalized epoch instant, so aware/naive-UTC agree.
        return round(value.timestamp(), 3)
    if isinstance(value, dict):
        return {k: canonical(v) for k, v in sorted(value.items(), key=repr)}

    # Remote time values are `EventTime` objects; local returns plain ints. Reduce
    # to the timestamp so the two sides compare. (`event_id` refinements are
    # exercised explicitly elsewhere, not through this default reduction.)
    t = getattr(value, "t", None)
    if isinstance(t, int) and not isinstance(value, (int, str)):
        return t

    # Entities: edge before node (an edge also has no `.name`).
    src, dst = getattr(value, "src", None), getattr(value, "dst", None)
    if src is not None and dst is not None:
        return ("edge", canonical(src.name), canonical(dst.name))
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return ("node", name)

    # Anything iterable (collections, histories) → order-insensitive list.
    try:
        items = [canonical(v) for v in value]
    except TypeError:
        return value
    return sorted(items, key=repr)


def _run(fn, g):
    """Return ``(result, None)`` or ``(None, exception)`` from ``fn(g)``."""
    try:
        return fn(g), None
    except Exception as exc:  # noqa: BLE001 — parity check needs the type
        return None, exc


def assert_parity(pair, fn):
    """Run ``fn`` on both graphs and assert the results are equivalent.

    Value parity: canonicalized results must be equal. Exception parity: if
    either side raises, both must raise the same exception type.
    """
    local_result, local_exc = _run(fn, pair.local)
    remote_result, remote_exc = _run(fn, pair.remote)

    if local_exc is not None or remote_exc is not None:
        assert type(local_exc) is type(remote_exc), (
            f"exception parity mismatch: "
            f"local raised {local_exc!r}, remote raised {remote_exc!r}"
        )
        return

    assert canonical(local_result) == canonical(
        remote_result
    ), f"value parity mismatch: local={local_result!r} remote={remote_result!r}"


# --- divergence ledger ------------------------------------------------------

# Known local↔remote gaps (see docs/remote-graph-api parity notes). A
# parametrized case tagged with one of these keys is xfailed with its reason,
# so the gap is *recorded* rather than silently skipped. Delete an entry when
# the corresponding remote API lands.
KNOWN_GAPS = {
    "nodes.history": "collection .history missing on remote (NodeState subsystem)",
    "edges.history": "collection .history missing on remote (NodeState subsystem)",
    "edges.deletions": "collection .deletions missing on remote",
    "expanding": "expanding() missing on remote for all view types",
    "rolling": "rolling() missing on remote for all view types",
    "path_from_graph.write": "PathFromGraph write mutators missing on remote",
    "nested_edges.write": "NestedEdges write mutators missing on remote",
    # Write-path gaps. These run in the *other* direction to the ones above:
    # remote has the API and the local `Graph` does not, so the drop-in surface
    # is still whole — but a graph-agnostic `build` cannot use them, which is
    # exactly what a ledger entry is for.
    "graph.add_nodes": (
        "batch add_nodes exists on RemoteGraph only; local Graph has no batch "
        "write API, so batch writes are compared against the equivalent loop"
    ),
    "graph.add_edges": (
        "batch add_edges exists on RemoteGraph only; local Graph has no batch "
        "write API, so batch writes are compared against the equivalent loop"
    ),
    "temporal_property.latest": (
        "RemoteTemporalProperty.latest() has no local TemporalProperty "
        "equivalent (local exposes latest() on TemporalProperties only)"
    ),
    # Filter-expression gaps (see test_parity_filters.py). These are genuine
    # local↔remote *disagreements*: an expression the local engine accepts is
    # refused by the remote lowering, so the same program means different things
    # on the two sides.
    #
    # All three share one root cause. Locally, `collection[expr]` is *always*
    # membership selection — it keeps the members the expression leaves visible
    # and hands them back over the unrestricted graph — and it accepts any
    # expression, graph views included. On the wire, membership selection is the
    # `select` field, whose argument is kind-typed (`NodeFilter` / `EdgeFilter`),
    # so an expression that is not kind-typed has no spelling. The neighbouring
    # `filter` field is NOT a substitute: it rescopes the collection rather than
    # narrowing it, so members that the expression excludes stay in the result.
    # Closing these needs a server field that applies a general `GqlFilter` with
    # select semantics; there is no client-only lowering.
    "filter.exploded_edge.props": (
        "ExplodedEdge property and metadata filters are refused remotely "
        "(ValueError: Not supported) but accepted locally; the ExplodedEdge "
        "predicates (is_valid / is_deleted / is_self_loop) do cross the wire "
        "because they also export as plain edge filters. The property form has "
        "no wire representation at all: FilterTree (the transportable export) "
        "has no ExplodedEdge variant, and the GraphQL schema has no "
        "exploded-edge filter input type"
    ),
    # An entity-type-mismatched `[expr]` used to be a fourth entry here: both
    # sides refused it, but as different exception types. The remote now raises
    # the same Exception('Node filter expected') the local engine does, so the
    # case is an ordinary assertion in test_parity_filters.py
    # (`test_edge_expr_in_a_node_subscript_is_refused_the_same_way`).
    # Remote-only filter application sites. Like the batch-write entries above,
    # these run in the other direction: the remote has the API and the local
    # handle does not, so a graph-agnostic case cannot exercise them.
    "filter.edges.filter": (
        "RemoteEdges.filter has no local counterpart; locally filter() is a "
        "node-view-op plus GraphView, so Edges has no filter method"
    ),
    "filter.edge.filter": (
        "RemoteEdge.filter has no local counterpart (as filter.edges.filter)"
    ),
    "filter.nested_edges.filter": (
        "RemoteNestedEdges.filter has no local counterpart (as " "filter.edges.filter)"
    ),
    "filter.collection.select": (
        "select() — the remote's explicit narrow-here-only form — has no local "
        "counterpart; locally only the collection[expr] sugar exists"
    ),
    "filter.node.by_state_column": (
        "filter.Node.by_state_column needs a boolean OutputNodeState column, "
        "and no algorithm on the drop-in surface produces one, so the "
        "expression cannot be built for either side to apply"
    ),
}
