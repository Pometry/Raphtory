"""Local vs remote API parity: shared fixture, comparator, and divergence ledger.

The premise of RemoteGraph is that it is a drop-in replacement for the local
``Graph``. That gives differential tests a free oracle: a single ``build(g)``
seeds *both* graphs, a single call runs on *both*, and — once canonicalized —
their results must be equal. The local ``Graph`` is ground truth.

Three pieces live here:

* ``graph_pair`` — seed a local ``Graph`` and a ``RemoteGraph`` identically and
  yield both.
* ``canonical`` / ``assert_parity`` — reduce a result to a comparable form and
  assert local == remote, including exception parity. **No drift is tolerated**:
  the comparator may only bridge the fact that the two sides are distinct
  objects over distinct graphs (see its docstring). Anything else that differs
  is a product bug and belongs in ``KNOWN_GAPS`` with an issue.
* ``KNOWN_GAPS`` — the divergence ledger. APIs not yet at parity are recorded
  here (with a reason) rather than silently skipped, so the suite doubles as an
  executable parity spec that shrinks as gaps close.
"""

from __future__ import annotations

import contextlib
import datetime
import math
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass

from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer

# The graph models a pair can be built for. These strings select the model on
# *both* sides, but asymmetrically: remotely `graph_type` is an argument to
# `new_graph` and the handle is a `RemoteGraph` either way (the model is
# server-side state), while locally the model *is* the class. So the same string
# has to be routed two different ways, and a pair built from mismatched halves
# would compare two graph models and report the difference as a parity bug.
GRAPH_TYPES = ("EVENT", "PERSISTENT")

_LOCAL_CLASS = {"EVENT": Graph, "PERSISTENT": PersistentGraph}


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

    ``graph_type`` picks the graph model for *both* sides: the server is told
    which kind to create and the local side is instantiated from the matching
    class, so the pair always compares like with like.
    """
    try:
        local_cls = _LOCAL_CLASS[graph_type]
    except KeyError:
        raise ValueError(
            f"unknown graph_type {graph_type!r}, expected one of {list(GRAPH_TYPES)}"
        )
    local = local_cls()
    build(local)
    # TemporaryDirectory (outer) is torn down only after the server context
    # (inner) has stopped and flushed — so the dir outlives every write-back and
    # is then cleaned up, rather than leaked as `mkdtemp` would.
    with tempfile.TemporaryDirectory() as work_dir:
        with GraphServer(work_dir).start() as server:
            client = server.get_client()
            remote = client.new_graph("g", graph_type)
            build(remote)
            yield GraphPair(local=local, remote=remote)


# --- canonicalization -------------------------------------------------------

# Time classes that compare equal to *each other* and to a bare ``int``:
# ``OptionalEventTime(1) == EventTime(1) == 1`` are all true, and an empty
# ``OptionalEventTime`` equals ``None``. So value comparison alone cannot see a
# substituted class — while the classes differ where it matters to callers
# (``is None`` flips, and ``is_none()`` exists on only one of them). The
# comparator therefore carries the class alongside the value for these, so a
# remote that swapped one for the other, or degraded to a bare int or ``None``,
# fails instead of passing silently.
_TIME_TYPES = frozenset({"OptionalEventTime", "EventTime"})


def _identity(value):
    """``('edge', src, dst)`` / ``('node', name)`` for an entity, else ``None``."""
    src, dst = getattr(value, "src", None), getattr(value, "dst", None)
    if src is not None and dst is not None:
        return ("edge", src.name, dst.name)
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return ("node", name)
    return None


def canonical(value):
    """Reduce a raphtory result to a comparable Python value.

    This is deliberately *not* a normalizer. It bridges only the ways in which
    the two sides are unavoidably different objects, never a difference in the
    answers themselves:

    1. **Entities become identities.** A local ``Node`` and a remote
       ``RemoteNode`` point at different graphs, so they could never compare
       equal; they are reduced to name / ``(src, dst)``.
    2. **Containers are materialized, never reordered.** A local ``Nodes`` and a
       remote ``RemoteNodes`` are distinct classes, so ``==`` between them is
       meaningless; listing them makes their *contents* comparable. Order is
       left exactly as each side produced it, so an ordering difference is a
       failure rather than something the comparator absorbs. (Entity collections
       used to be sorted here on the grounds that iteration order is
       unspecified. Measured instead: local and remote agree on the order of
       nodes, edges, neighbours, windowed and subgraph collections, and the
       whole suite — including the generative properties — passes without the
       sort. So it was hiding nothing, and removing it means the suite would
       report it if that ever changed.)
    3. **Time values carry their class.** This one *tightens* the comparison
       rather than bridging a difference: the time classes compare equal to
       each other and to bare ints, so without the class a substituted return
       type would pass (see ``_TIME_TYPES``). The value itself is kept as-is
       alongside it, so nothing about the comparison is weakened.

    Nothing else is touched. Float precision, datetime timezone, ``NaN`` and
    map key order all compare as they come: if a value does not survive the
    round-trip exactly, that is a product bug for ``KNOWN_GAPS`` and an issue,
    not something to paper over here.
    """
    name = type(value).__name__
    if name in _TIME_TYPES:
        # Paired with the value, not a reduction of it: `==` on the value keeps
        # doing whatever it did before, and the class merely has to match too.
        return (name, value)

    identity = _identity(value)
    if identity is not None:
        return identity

    if isinstance(value, dict):
        # `dict.__eq__` already ignores key order, so keys are left alone; a
        # test that cares about map *order* asserts it explicitly.
        return {k: canonical(v) for k, v in value.items()}
    # str/bytes are Iterable but compare as scalars, never element-wise.
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [canonical(v) for v in value]
    return value


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
    # Two filter-expression gaps used to be ledgered here. ExplodedEdge
    # property/metadata filters are now transported (FilterTree gained an
    # ExplodedEdge kind and the schema an `ExplodedEdgeFilter` input), so they
    # are ordinary matrix entries in test_parity_filters.py. And an
    # entity-type-mismatched `[expr]` was refused by both sides but as
    # different exception types; the remote now raises the same
    # Exception('Node filter expected') the local engine does, so the case is
    # an ordinary assertion there too
    # (`test_edge_expr_in_a_node_subscript_is_refused_the_same_way`).
    # The Edge / Edges / NestedEdges `filter` sites used to be ledgered here as
    # remote-only. They are ordinary matrix entries in test_parity_filters.py
    # now that the local handles accept a filter too (`FILTER_SITES`).
    # `select()` used to be ledgered here as remote-only. It is remote-only, but
    # that is an additive extra rather than a divergence: the `collection[expr]`
    # sugar both sides share lowers to the same server field, and is covered as
    # an ordinary matrix in test_parity_filters.py (`GETITEM_SITES`).
    "collection_props.temporal": (
        "the collection-level PropertiesView.temporal columnar timeline view "
        "is not implemented on remote (deferred with the NodeState subsystem)"
    ),
    "history.merge": (
        "History.merge / History.compose_histories are unavailable on remote "
        "history handles — combining histories needs either server support or "
        "client-side merge semantics (deferred with the NodeState subsystem)"
    ),
    "filter.node.by_state_column": (
        "filter.Node.by_state_column needs a boolean OutputNodeState column, "
        "and no algorithm on the drop-in surface produces one, so the "
        "expression cannot be built for either side to apply"
    ),
}
