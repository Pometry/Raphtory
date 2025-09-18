from datetime import datetime, timezone
from itertools import islice
import pytest
from raphtory import Graph
from utils import run_group_graphql_test

@pytest.fixture()
def example_graph() -> Graph:
    g = Graph()
    dt1 = datetime(2025, 3, 15, 14, 37, 52)     # March 15
    dt2 = datetime(2025, 7, 8, 9, 12, 5)        # July 8
    dt3 = datetime(2025, 11, 22, 21, 45, 30)    # November 22

    g.add_node(dt1, 1)
    g.add_node(dt2, 1)
    g.add_node(dt3, 1)

    return g

def test_rolling_month_alignment_default_true(example_graph):
    g = example_graph
    windows = list(islice(g.rolling("1 month"), 3))  # align_start defaults to True

    # Expect alignment to month starts: 2025-03-01, 2025-04-01, 2025-05-01
    exp0_start = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end   = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end   = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end   = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Validate the first three windows
    for i, (exp_start, exp_end) in enumerate([(exp0_start, exp0_end),
                                        (exp1_start, exp1_end),
                                        (exp2_start, exp2_end)]):
        w = windows[i]
        start = w.start_date_time
        end = w.end_date_time
        assert start == exp_start, f"window[{i}] start: {start} != {exp_start}"
        assert end == exp_end, f"window[{i}] end: {end} != {exp_end}"

def test_rolling_day_alignment_default_true(example_graph):
    g = example_graph
    windows = list(islice(g.rolling("1 day"), 3))  # align_start defaults to True

    exp0_start = datetime(2025, 3, 15, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end   = datetime(2025, 3, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 3, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end   = datetime(2025, 3, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 3, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end   = datetime(2025, 3, 18, 0, 0, 0, 0, tzinfo=timezone.utc)

    s0, e0 = windows[0].start_date_time, windows[0].end_date_time
    s1, e1 = windows[1].start_date_time, windows[1].end_date_time
    s2, e2 = windows[2].start_date_time, windows[2].end_date_time
    assert s0 == exp0_start and e0 == exp0_end
    assert s1 == exp1_start and e1 == exp1_end
    assert s2 == exp2_start and e2 == exp2_end

def test_rolling_month_and_day_alignment_default_true(example_graph):
    g = example_graph
    windows = list(islice(g.rolling("1 month and 1 day"), 3))  # align_start defaults to True

    exp0_start = datetime(2025, 3, 15, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end   = datetime(2025, 4, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 4, 16, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end   = datetime(2025, 5, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 5, 17, 0, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end   = datetime(2025, 6, 18, 0, 0, 0, 0, tzinfo=timezone.utc)

    s0, e0 = windows[0].start_date_time, windows[0].end_date_time
    s1, e1 = windows[1].start_date_time, windows[1].end_date_time
    s2, e2 = windows[2].start_date_time, windows[2].end_date_time
    assert s0 == exp0_start and e0 == exp0_end
    assert s1 == exp1_start and e1 == exp1_end
    assert s2 == exp2_start and e2 == exp2_end

def test_rolling_alignment_smallest_of_window_and_step(example_graph):
    g = example_graph
    windows = list(g.rolling("1 month", step="1 day"))[:10]  # align_start True
    window = windows[0]
    print(f"\nGraph earliest time: {g.earliest_date_time}")
    for gw in windows:
        print(f"Start: {gw.start_date_time}; End: {gw.end_date_time}")
    # FIXME: the first window is 2025-02-16 -> 2025-03-16. Shouldn't it be 2025-03-15 -> 2025-04-15?
    # Earliest event is 2025-03-15 14:37:52; day alignment (not month): 2025-03-15 00:00:00
    assert window.start_date_time == datetime(2025, 2, 16, 0, 0, 0, 0, tzinfo=timezone.utc)

def test_rolling_no_alignment_for_discrete_ms(example_graph):
    g = example_graph
    # Discrete window (milliseconds), alignment_unit is None so no alignment
    window = next(g.rolling(1000).__iter__())  # 1 second window, align_start True but discrete
    # expect no alignment
    assert window.start_date_time == datetime(2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc)

def test_rolling_align_start_false(example_graph):
    g = example_graph
    window = next(g.rolling("1 month", align_start=False).__iter__())
    # no month alignment
    assert window.start_date_time == datetime(2025, 3, 15, 14, 37, 52, tzinfo=timezone.utc)

def test_expanding_day_alignment_default_true(example_graph):
    g = example_graph
    ws = list(islice(g.expanding("1 day"), 2))  # align_start True
    # With expanding, start is unbounded (None) on an unwindowed graph, so validate ends align to day boundaries
    exp_end0 = datetime(2025, 3, 16, 0, 0, 0, tzinfo=timezone.utc)
    exp_end1 = datetime(2025, 3, 17, 0, 0, 0, tzinfo=timezone.utc)
    assert ws[0].end_date_time == exp_end0
    assert ws[1].end_date_time == exp_end1

def test_expanding_align_start_false(example_graph):
    g = example_graph
    ws = next(g.expanding("1 day", align_start=False).__iter__())
    exp_end0 = datetime(2025, 3, 16, 14, 37, 52, tzinfo=timezone.utc)
    assert ws.end_date_time == exp_end0

def test_week_alignment_epoch_buckets():
    g = Graph()
    g.add_edge("1970-01-10 12:00:00", 0, 1)  # 9.5 days after epoch
    # weeks align to multiples of 7 days since 1970-01-01 00:00:00 UTC
    ws = next(g.rolling("1 week").__iter__())
    assert ws.start_date_time == datetime(1970, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
    assert ws.end_date_time == datetime(1970, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

def test_rolling_month_alignment_with_layers():
    g = Graph()
    # events in February only
    g.add_edge("2025-02-01 10:00:00", 10, 20, layer="february")
    g.add_edge("2025-02-15 12:00:00", 20, 30, layer="february")
    # events in March and May
    g.add_edge("2025-03-15 14:37:52", 1, 2, layer="march-may")
    g.add_edge("2025-05-20 00:00:00", 2, 3, layer="march-may")

    windows = list(islice(g.layer("march-may").rolling("1 month"), 3))

    # layer "february" shouldn't affect window start
    exp0_start = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp0_end   = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_start = datetime(2025, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp1_end   = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_start = datetime(2025, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    exp2_end   = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    w0, w1, w2 = windows[0], windows[1], windows[2]
    assert w0.start_date_time == exp0_start and w0.end_date_time == exp0_end
    assert w1.start_date_time == exp1_start and w1.end_date_time == exp1_end
    assert w2.start_date_time == exp2_start and w2.end_date_time == exp2_end
