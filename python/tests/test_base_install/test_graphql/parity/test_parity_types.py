"""Return *types* must match, not just return values.

Value parity has a blind spot: raphtory's time types compare equal across
classes, so ``OptionalEventTime(Some(1)) == EventTime(1)`` is ``True``. A remote
accessor could therefore hand back a different class from its local counterpart
and every value assertion in this suite would still pass — while real user code
broke, because the two classes disagree about ``is None``, ``.is_none()`` and
``x.t`` on an empty value.

So the drop-in claim needs a second, type-level check. Each case below reaches
the same accessor on both sides and asserts the *classes* agree, both when a
value is present and — where reachable — when it is absent.
"""

import pytest

from _parity import GRAPH_TYPES, graph_pair


def _build(g):
    g.add_node(1, "a")
    g.add_node(5, "b")
    g.add_node(6, "c")
    g.add_edge(3, "a", "b")
    g.add_edge(7, "a", "c")


@pytest.fixture(scope="module")
def pair():
    with graph_pair(_build) as p:
        yield p


# Accessors that yield a single optional time. Locally every one of these is an
# `OptionalEventTime` — one static class whether or not a time exists — so the
# remote must not substitute `EventTime`/`None`.
SCALAR_TIME_ACCESSORS = {
    "graph.earliest_time": lambda g: g.earliest_time,
    "graph.latest_time": lambda g: g.latest_time,
    "graph.start": lambda g: g.start,
    "graph.end": lambda g: g.end,
    "node.earliest_time": lambda g: g.node("a").earliest_time,
    "node.latest_time": lambda g: g.node("a").latest_time,
    "node.start": lambda g: g.node("a").start,
    "node.end": lambda g: g.node("a").end,
    "edge.earliest_time": lambda g: g.edge("a", "b").earliest_time,
    "edge.latest_time": lambda g: g.edge("a", "b").latest_time,
    "edge.start": lambda g: g.edge("a", "b").start,
    "edge.end": lambda g: g.edge("a", "b").end,
    "nodes.start": lambda g: g.nodes.start,
    "nodes.end": lambda g: g.nodes.end,
    "edges.start": lambda g: g.edges.start,
    "edges.end": lambda g: g.edges.end,
    "history.earliest_time": lambda g: g.node("a").history.earliest_time(),
    "history.latest_time": lambda g: g.node("a").history.latest_time(),
}

# Accessors that yield a *collection* of times. Locally the elements are bare
# `EventTime` (an entity in a collection always has events), so wrapping them
# in an optional remotely would be just as much a divergence as the reverse.
COLLECTION_TIME_ACCESSORS = {
    "nodes.earliest_time": lambda g: list(g.nodes.earliest_time),
    "nodes.latest_time": lambda g: list(g.nodes.latest_time),
    "edges.earliest_time": lambda g: list(g.edges.earliest_time),
    "edges.latest_time": lambda g: list(g.edges.latest_time),
    "node.history": lambda g: list(g.node("a").history),
    "path.earliest_time": lambda g: list(g.node("a").neighbours.earliest_time),
    "nested.earliest_time": lambda g: [
        x for row in g.nodes.neighbours.earliest_time for x in row
    ],
}


def _types(pair, fn):
    """``(local_type, remote_type)`` for ``fn`` applied to each side."""
    return type(fn(pair.local)), type(fn(pair.remote))


@pytest.mark.parametrize("name", sorted(SCALAR_TIME_ACCESSORS))
def test_scalar_time_accessor_types_match(pair, name):
    local, remote = _types(pair, SCALAR_TIME_ACCESSORS[name])
    assert local is remote, (
        f"{name} returns {local.__name__} locally but {remote.__name__} remotely; "
        f"the two classes disagree about `is None` / `.is_none()` / `.t`, so this "
        f"is a drop-in break even though the values compare equal"
    )


@pytest.mark.parametrize("name", sorted(COLLECTION_TIME_ACCESSORS))
def test_collection_time_element_types_match(pair, name):
    fn = COLLECTION_TIME_ACCESSORS[name]
    local_items, remote_items = fn(pair.local), fn(pair.remote)
    assert local_items and remote_items, f"{name}: nothing to compare"
    local_types = {type(x).__name__ for x in local_items}
    remote_types = {type(x).__name__ for x in remote_items}
    assert local_types == remote_types, (
        f"{name} yields {sorted(local_types)} locally but "
        f"{sorted(remote_types)} remotely"
    )


# Accessors with no value to report, and the models they are empty in. An
# unbounded view has no bounds under either model. A windowed-out view has no
# earliest time only in an event graph — under `PERSISTENT` the edges persist
# into the window, so it reports the window start instead of nothing.
ABSENT_TIME_ACCESSORS = {
    "graph.start (unbounded)": (lambda g: g.start, GRAPH_TYPES),
    "graph.end (unbounded)": (lambda g: g.end, GRAPH_TYPES),
    "graph.earliest_time (empty window)": (
        lambda g: g.window(500, 600).earliest_time,
        ("EVENT",),
    ),
    "graph.latest_time (empty window)": (
        lambda g: g.window(500, 600).latest_time,
        ("EVENT",),
    ),
}


@pytest.mark.parametrize("graph_type", GRAPH_TYPES)
def test_absent_time_types_match(graph_type):
    """The *empty* case too — that is where the classes used to diverge.

    Locally an absent time is still an `OptionalEventTime`; the remote used to
    collapse to a bare `None`, which flips `is None` and loses `.is_none()` and
    `.t`. Both halves are asserted: the classes agree, and each side really is
    reporting *absence* — otherwise the case would prove nothing.
    """
    with graph_pair(_build, graph_type=graph_type) as pair:
        for name, (fn, models) in ABSENT_TIME_ACCESSORS.items():
            if graph_type not in models:
                continue
            local, remote = type(fn(pair.local)), type(fn(pair.remote))
            assert (
                local is remote
            ), f"{name} is {local.__name__} locally but {remote.__name__} remotely"
            for side_name, side in (("local", pair.local), ("remote", pair.remote)):
                value = fn(side)
                assert value.is_none(), f"{side_name} {name}: expected an empty time"
                assert (
                    value is not None
                ), f"{side_name} {name}: collapsed to a bare None"
