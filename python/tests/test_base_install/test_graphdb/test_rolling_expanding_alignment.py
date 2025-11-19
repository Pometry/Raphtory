from datetime import datetime, timezone
import pytest
from raphtory import Graph
from utils import run_group_graphql_test


@pytest.fixture()
def example_graph() -> Graph:
    g = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)  # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    return g


@pytest.fixture()
def example_graph_with_edges() -> Graph:
    g = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)  # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)  # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)  # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    g.add_edge(datetime(2025, 3, 15, 15, 0, 0), 1, 2)
    g.add_edge(datetime(2025, 3, 16, 1, 0, 0), 2, 3)

    return g


def test_rolling_month_alignment_default_true(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month"))  # align_start defaults to True

    # Expect alignment to month starts: 2025-03-01, 2025-04-01, 2025-05-01
    exp0_start = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Validate the first three windows
    for i, (exp_start, exp_end) in enumerate(
        [(exp0_start, exp0_end), (exp1_start, exp1_end), (exp2_start, exp2_end)]
    ):
        w = windows[i]
        start = w.start_date_time
        end = w.end_date_time
        assert start == exp_start, f"window[{i}] start: {start} != {exp_start}"
        assert end == exp_end, f"window[{i}] end: {end} != {exp_end}"

    # check last window for errors at boundary. Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 1, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 12, 1, 0, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_day_alignment_default_true(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 day"))  # align_start defaults to True

    exp0_start = datetime(2025, 3, 15, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end = datetime(2025, 3, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 3, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end = datetime(2025, 3, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 3, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end = datetime(2025, 3, 18, 0, 0, 0, 0, tzinfo=timezone.utc)

    s0, e0 = windows[0].start_date_time, windows[0].end_date_time
    s1, e1 = windows[1].start_date_time, windows[1].end_date_time
    s2, e2 = windows[2].start_date_time, windows[2].end_date_time
    assert s0 == exp0_start and e0 == exp0_end
    assert s1 == exp1_start and e1 == exp1_end
    assert s2 == exp2_start and e2 == exp2_end

    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 22, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_month_and_day_alignment_default_true(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month and 1 day"))  # align_start defaults to True

    exp0_start = datetime(2025, 3, 15, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end = datetime(2025, 4, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 4, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end = datetime(2025, 5, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 5, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end = datetime(2025, 6, 18, 0, 0, 0, 0, tzinfo=timezone.utc)

    s0, e0 = windows[0].start_date_time, windows[0].end_date_time
    s1, e1 = windows[1].start_date_time, windows[1].end_date_time
    s2, e2 = windows[2].start_date_time, windows[2].end_date_time
    assert s0 == exp0_start and e0 == exp0_end
    assert s1 == exp1_start and e1 == exp1_end
    assert s2 == exp2_start and e2 == exp2_end

    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 10, 22, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_alignment_smallest_of_window_and_step(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month", step="1 day"))
    # Earliest event is 2025-03-15 14:37:52; day alignment (not month): 2025-03-15 00:00:00
    assert windows[0].start_date_time == datetime(
        2025, 2, 16, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 3, 16, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[1].start_date_time == datetime(
        2025, 2, 17, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[1].end_date_time == datetime(
        2025, 3, 17, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[2].start_date_time == datetime(
        2025, 2, 18, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[2].end_date_time == datetime(
        2025, 3, 18, 0, 0, 0, 0, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 10, 23, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_no_alignment_for_discrete_ms(example_graph):
    g: Graph = example_graph
    # Discrete window (1 day in milliseconds), align_start = true but alignment_unit = None so no alignment
    windows = list(g.rolling(86400000))
    # expect no alignment
    assert windows[0].start_date_time == datetime(
        2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 3, 16, 14, 37, 52, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 22, 14, 37, 52, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 11, 23, 14, 37, 52, tzinfo=timezone.utc
    )


def test_rolling_align_start_false(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month", alignment_unit="unaligned"))
    # no month alignment
    assert windows[0].start_date_time == datetime(
        2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 4, 15, 14, 37, 52, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 15, 14, 37, 52, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 12, 15, 14, 37, 52, tzinfo=timezone.utc
    )


def test_rolling_custom_align_day(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month", alignment_unit="day"))
    # no month alignment
    assert windows[0].start_date_time == datetime(
        2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 4, 15, 0, 0, 0, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 15, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 12, 15, 0, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_custom_align_hour(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month", alignment_unit="hours"))
    # no month alignment
    assert windows[0].start_date_time == datetime(
        2025, 3, 15, 14, 0, 0, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 4, 15, 14, 0, 0, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 11, 15, 14, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 12, 15, 14, 0, 0, tzinfo=timezone.utc
    )


def test_rolling_custom_align_day_with_step(example_graph):
    g: Graph = example_graph
    windows = list(g.rolling("1 month", step="1 week", alignment_unit="day"))
    # no month alignment
    assert windows[0].start_date_time == datetime(
        2025, 2, 22, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[0].end_date_time == datetime(
        2025, 3, 22, 0, 0, 0, tzinfo=timezone.utc
    )
    # Latest time is 2025-11-22 21:45:30
    assert windows[-1].start_date_time == datetime(
        2025, 10, 29, 0, 0, 0, tzinfo=timezone.utc
    )
    assert windows[-1].end_date_time == datetime(
        2025, 11, 29, 0, 0, 0, tzinfo=timezone.utc
    )


def test_expanding_day_alignment_default_true(example_graph):
    g: Graph = example_graph
    ws = list(g.expanding("1 day"))  # align_start True
    # With expanding, start is unbounded (None) on an unwindowed graph, so validate ends align to day boundaries
    exp_end0 = datetime(2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == exp_end0
    assert ws[1].end_date_time == exp_end1

    # Latest time is 2025-11-22 21:45:30
    exp_end_last = datetime(2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == exp_end_last


def test_expanding_align_start_false(example_graph):
    g: Graph = example_graph
    ws = list(g.expanding("1 day", alignment_unit="unaligned"))
    exp_end0 = datetime(2025, 3, 16, 14, 37, 52, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 23, 14, 37, 52, tzinfo=timezone.utc)
    assert ws[0].end_date_time == exp_end0
    assert ws[-1].end_date_time == exp_end_last


def test_expanding_custom_align_month(example_graph):
    g: Graph = example_graph
    ws = list(g.expanding("1 day", alignment_unit="month"))
    exp_end0 = datetime(2025, 3, 2, 0, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == exp_end0
    assert ws[-1].end_date_time == exp_end_last


def test_expanding_custom_align_week(example_graph):
    g: Graph = example_graph
    ws = list(g.expanding("1 day", alignment_unit="weeks"))
    exp_end0 = datetime(2025, 3, 14, 0, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 23, 0, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == exp_end0
    assert ws[-1].end_date_time == exp_end_last


def test_week_alignment_epoch_buckets():
    g = Graph()
    g.add_edge("1970-01-10 12:00:00", 0, 1)  # 9.5 days after epoch
    # weeks align to multiples of 7 days since 1970-01-01 00:00:00 UTC
    ws = list(g.rolling("1 week"))
    exp_start0 = datetime(1970, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(1970, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    # only one window
    assert ws[0].start_date_time == exp_start0
    assert ws[0].end_date_time == exp_end0
    assert ws[-1].start_date_time == exp_start0
    assert ws[-1].end_date_time == exp_end0


def test_rolling_month_alignment_with_layers():
    g: Graph = Graph()
    # events in February only
    g.add_edge("2025-02-01 10:00:00", 10, 20, layer="february")
    g.add_edge("2025-02-15 12:00:00", 20, 30, layer="february")
    # events in March and May
    g.add_edge("2025-03-15 14:37:52", 1, 2, layer="march-may")
    g.add_edge("2025-05-20 00:00:00", 2, 3, layer="march-may")

    windows = list(g.layer("march-may").rolling("1 month"))

    # layer "february" affects window start
    exp0_start = datetime(2025, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp3_start = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp3_end = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    w0, w1, w2, w3, w_last = windows[0], windows[1], windows[2], windows[3], windows[-1]
    assert w0.start_date_time == exp0_start and w0.end_date_time == exp0_end
    assert w1.start_date_time == exp1_start and w1.end_date_time == exp1_end
    assert w2.start_date_time == exp2_start and w2.end_date_time == exp2_end
    assert w3.start_date_time == exp3_start and w3.end_date_time == exp3_end
    # only 3 windows
    assert w_last.start_date_time == exp3_start and w_last.end_date_time == exp3_end


def test_node_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    n1 = g.node(1)
    assert n1 is not None
    windows = list(n1.rolling("1 day"))
    exp_start0 = datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    exp_start_last = datetime(2025, 11, 22, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)

    assert (
        windows[0].start_date_time == exp_start0
        and windows[0].end_date_time == exp_end0
    )
    assert (
        windows[1].start_date_time == exp_start1
        and windows[1].end_date_time == exp_end1
    )
    # Latest time is 2025-11-22 21:45:30
    assert (
        windows[-1].start_date_time == exp_start_last
        and windows[-1].end_date_time == exp_end_last
    )

    expand_windows = list(n1.expanding("1 day"))
    assert expand_windows[0].end_date_time == exp_end0
    assert expand_windows[1].end_date_time == exp_end1
    assert expand_windows[-1].end_date_time == exp_end_last


def test_nodes_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    # there is only one node "1"
    ws = list(g.nodes.rolling("1 day"))
    assert ws[0].start_date_time == datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    assert ws[-1].start_date_time == datetime(2025, 11, 22, 0, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)

    exp_ws = list(g.nodes.expanding("1 day"))
    assert exp_ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)


def test_edge_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    e = g.edge(1, 2)
    assert e is not None
    ws = list(e.rolling("1 day"))
    assert ws[0].start_date_time == datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    # even if we call it on an edge, we still get all windows where the underlying graph is valid
    assert ws[-1].start_date_time == datetime(2025, 11, 22, 0, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)

    exp_ws = list(e.expanding("1 day"))
    assert exp_ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)


def test_edges_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    ws = list(g.edges.rolling("1 day"))
    assert ws[0].start_date_time == datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    # even if we call it on an edge, we still get all windows where the underlying graph is valid
    assert ws[-1].start_date_time == datetime(2025, 11, 22, 0, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)

    exp_ws = list(g.edges.expanding("1 day"))
    assert exp_ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    assert exp_ws[-1].end_date_time == datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)


def test_path_from_node_neighbours_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    n1 = g.node(1)
    assert n1 is not None
    ws = list(n1.neighbours.rolling("1 hour"))
    assert ws[0].start_date_time == datetime(2025, 3, 15, 14, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 15, 15, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 15, 15, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 15, 16, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    # even if we call it on an edge, we still get all windows where the underlying graph is valid
    assert ws[-1].start_date_time == datetime(2025, 11, 22, 21, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 22, 22, 0, tzinfo=timezone.utc)

    exp_ws = list(n1.neighbours.expanding("1 hour"))
    assert exp_ws[0].end_date_time == datetime(2025, 3, 15, 15, 0, tzinfo=timezone.utc)
    assert exp_ws[1].end_date_time == datetime(2025, 3, 15, 16, 0, tzinfo=timezone.utc)
    assert exp_ws[-1].end_date_time == datetime(
        2025, 11, 22, 22, 0, tzinfo=timezone.utc
    )


def test_path_from_graph_neighbours_alignment(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    # there is only one node
    neigh = g.nodes.neighbours
    ws = list(neigh.rolling("2 hours"))
    assert ws[0].start_date_time == datetime(2025, 3, 15, 14, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 15, 16, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 15, 16, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 15, 18, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    assert ws[-1].start_date_time == datetime(2025, 11, 22, 20, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 22, 22, 0, tzinfo=timezone.utc)

    exp_ws = list(neigh.expanding("2 hours"))
    assert exp_ws[0].end_date_time == datetime(2025, 3, 15, 16, 0, tzinfo=timezone.utc)
    assert exp_ws[1].end_date_time == datetime(2025, 3, 15, 18, 0, tzinfo=timezone.utc)
    assert exp_ws[-1].end_date_time == datetime(
        2025, 11, 22, 22, 0, tzinfo=timezone.utc
    )


def test_mismatched_window_step_basic(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    # window: 1 day (temporal), step: 1 hour (discrete epoch ms)
    ws = list(g.rolling("1 day", step=3600000))
    # earliest time is 2025-03-15 14:37:52
    exp_start0 = datetime(2025, 3, 14, 15, 37, 52, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 3, 15, 15, 37, 52, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 3, 14, 16, 37, 52, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 15, 16, 37, 52, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    exp_start_last = datetime(2025, 11, 21, 22, 37, 52, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 22, 22, 37, 52, tzinfo=timezone.utc)
    assert ws[0].start_date_time == exp_start0 and ws[0].end_date_time == exp_end0
    assert ws[1].start_date_time == exp_start1 and ws[1].end_date_time == exp_end1
    assert (
        ws[-1].start_date_time == exp_start_last
        and ws[-1].end_date_time == exp_end_last
    )


def test_mismatched_window_step_basic2(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    # window: 1 hour (discrete epoch ms), step: 1 day (temporal)
    ws = list(g.rolling(3600000, step="1 day"))
    # Earliest time: 2025-03-15 14:37:52
    # The first window is past the first item because we are aligning when the step is larger than the window.
    assert ws[0].start_date_time == datetime(2025, 3, 15, 23, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == datetime(2025, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert ws[1].start_date_time == datetime(2025, 3, 16, 23, 0, tzinfo=timezone.utc)
    assert ws[1].end_date_time == datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    # The last window is before the last item because we are aligning when the step is larger than the window.
    assert ws[-1].start_date_time == datetime(2025, 11, 21, 23, 0, tzinfo=timezone.utc)
    assert ws[-1].end_date_time == datetime(2025, 11, 22, 0, 0, tzinfo=timezone.utc)


def test_window_different_intervals_1(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    windows = list(g.rolling("3 weeks", step="2 days"))
    # Earliest time: 2025-03-15 14:37:52
    exp_start0 = datetime(2025, 2, 24, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 3, 17, 0, 0, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 2, 26, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 19, 0, 0, tzinfo=timezone.utc)
    exp_start2 = datetime(2025, 2, 28, 0, 0, tzinfo=timezone.utc)
    exp_end2 = datetime(2025, 3, 21, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    exp_start_last = datetime(2025, 11, 3, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 24, 0, 0, tzinfo=timezone.utc)
    assert (
        windows[0].start_date_time == exp_start0
        and windows[0].end_date_time == exp_end0
    )
    assert (
        windows[1].start_date_time == exp_start1
        and windows[1].end_date_time == exp_end1
    )
    assert (
        windows[2].start_date_time == exp_start2
        and windows[2].end_date_time == exp_end2
    )
    assert (
        windows[-1].start_date_time == exp_start_last
        and windows[-1].end_date_time == exp_end_last
    )


def test_window_different_intervals_2(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    windows = list(g.rolling("3 weeks and 2 days"))
    # Earliest time: 2025-03-15 14:37:52
    exp_start0 = datetime(2025, 3, 15, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 4, 7, 0, 0, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 4, 7, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    exp_start2 = datetime(2025, 4, 30, 0, 0, tzinfo=timezone.utc)
    exp_end2 = datetime(2025, 5, 23, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    exp_start_last = datetime(2025, 10, 31, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 23, 0, 0, tzinfo=timezone.utc)
    assert (
        windows[0].start_date_time == exp_start0
        and windows[0].end_date_time == exp_end0
    )
    assert (
        windows[1].start_date_time == exp_start1
        and windows[1].end_date_time == exp_end1
    )
    assert (
        windows[2].start_date_time == exp_start2
        and windows[2].end_date_time == exp_end2
    )
    assert (
        windows[-1].start_date_time == exp_start_last
        and windows[-1].end_date_time == exp_end_last
    )


def test_window_different_intervals_3(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    windows = list(g.rolling("3 weeks and 2 days", step="3 days"))
    # Earliest time: 2025-03-15 14:37:52
    exp_start0 = datetime(2025, 2, 23, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 3, 18, 0, 0, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 2, 26, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 21, 0, 0, tzinfo=timezone.utc)
    exp_start2 = datetime(2025, 3, 1, 0, 0, tzinfo=timezone.utc)
    exp_end2 = datetime(2025, 3, 24, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    exp_start_last = datetime(2025, 11, 2, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 25, 0, 0, tzinfo=timezone.utc)
    assert (
        windows[0].start_date_time == exp_start0
        and windows[0].end_date_time == exp_end0
    )
    assert (
        windows[1].start_date_time == exp_start1
        and windows[1].end_date_time == exp_end1
    )
    assert (
        windows[2].start_date_time == exp_start2
        and windows[2].end_date_time == exp_end2
    )
    assert (
        windows[-1].start_date_time == exp_start_last
        and windows[-1].end_date_time == exp_end_last
    )


def test_window_different_intervals_4(example_graph_with_edges):
    g: Graph = example_graph_with_edges
    windows = list(g.rolling("2 days", step="3 weeks and 4 days"))
    # Earliest time: 2025-03-15 14:37:52
    # The first window is past the first item because we are aligning when the step is larger than the window.
    exp_start0 = datetime(2025, 4, 7, 0, 0, tzinfo=timezone.utc)
    exp_end0 = datetime(2025, 4, 9, 0, 0, tzinfo=timezone.utc)
    exp_start1 = datetime(2025, 5, 2, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 5, 4, 0, 0, tzinfo=timezone.utc)
    exp_start2 = datetime(2025, 5, 27, 0, 0, tzinfo=timezone.utc)
    exp_end2 = datetime(2025, 5, 29, 0, 0, tzinfo=timezone.utc)
    # Latest time is 2025-11-22 21:45:30
    # The last window is before the last item because we are aligning when the step is larger than the window.
    exp_start_last = datetime(2025, 11, 18, 0, 0, tzinfo=timezone.utc)
    exp_end_last = datetime(2025, 11, 20, 0, 0, tzinfo=timezone.utc)
    assert (
        windows[0].start_date_time == exp_start0
        and windows[0].end_date_time == exp_end0
    )
    assert (
        windows[1].start_date_time == exp_start1
        and windows[1].end_date_time == exp_end1
    )
    assert (
        windows[2].start_date_time == exp_start2
        and windows[2].end_date_time == exp_end2
    )
    assert (
        windows[-1].start_date_time == exp_start_last
        and windows[-1].end_date_time == exp_end_last
    )
