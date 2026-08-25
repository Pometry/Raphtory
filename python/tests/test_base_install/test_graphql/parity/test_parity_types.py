"""Absent times report absence, on both sides, as a real time object.

The class-level half of this lives in the comparator: raphtory's time types
compare equal across classes and to bare ints, so ``canonical`` carries the
class alongside the value (see ``_TIME_TYPES`` in ``_parity.py``) and *every*
parity assertion in the suite rejects a substituted return type. Nothing here
needs to re-list accessors for that.

What the comparator cannot check is the one case below. An absent time is an
``OptionalEventTime`` that *reprs as* ``None`` and compares equal to it, while
``is None`` is ``False`` and ``is_none()`` is ``True``. Parity only says the two
sides agree; it cannot say they are both genuinely reporting absence rather
than both returning a present value. That is a per-side assertion, so it stays
explicit.
"""

import pytest

from _parity import GRAPH_TYPES, graph_pair


def _build(g):
    g.add_node(1, "a")
    g.add_node(5, "b")
    g.add_node(6, "c")
    g.add_edge(3, "a", "b")
    g.add_edge(7, "a", "c")


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
