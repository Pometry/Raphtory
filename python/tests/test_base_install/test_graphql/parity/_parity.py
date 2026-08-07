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

    assert canonical(local_result) == canonical(remote_result), (
        f"value parity mismatch: local={local_result!r} remote={remote_result!r}"
    )


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
    "fuzzy_filter": "FuzzySearch filter operator not representable over the wire",
}
