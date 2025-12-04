import pytest
from raphtory import EventTime, Graph
from datetime import datetime, timezone


@pytest.fixture()
def example_graph() -> Graph:
    g: Graph = Graph()
    g.add_node(datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 1)  # 0
    g.add_node(datetime(1970, 1, 2, 0, 0, 0, tzinfo=timezone.utc), 2)  # 86400000
    g.add_node(datetime(1970, 1, 2, 0, 30, 0, tzinfo=timezone.utc), 3)  # 88200000
    g.add_node(datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 4)  # 946684800000
    return g


def test_time_index():
    t = EventTime(5, 1)
    # check equality with int
    assert t > 4
    assert t == 5
    assert t < 6
    # check equality with tuple
    assert t > (5, 0)
    assert t == (5, 1)
    assert t < (5, 2)
    # check equality with datetime
    assert t > datetime(1970, 1, 1, 0, 0, 0, 0)
    assert t == datetime(1970, 1, 1, 0, 0, 0, 5000)
    assert t < datetime(1970, 1, 1, 0, 0, 0, 10000)
    # check equality with a tuple containing datetime
    assert t > (datetime(1970, 1, 1, 0, 0, 0, 5000), 0)
    assert t == (datetime(1970, 1, 1, 0, 0, 0, 5000), 1)
    assert t < (datetime(1970, 1, 1, 0, 0, 0, 5000), 2)
    # check equality with string
    t2 = EventTime(1000, 1)
    assert t2 > "1970-01-01 00:00:00"
    assert t2 == "1970-01-01 00:00:01"
    assert t2 < "1970-01-01 00:00:02"
    # check equality with a tuple containing datetime and string
    assert t > (datetime(1970, 1, 1, 0, 0, 0, 5000), "1970-01-01")
    assert t == (datetime(1970, 1, 1, 0, 0, 0, 5000), "1970-01-01T00:00:00.001")
    assert t < (datetime(1970, 1, 1, 0, 0, 0, 5000), "1970-01-01T00:00:00.010Z")


def test_time_input_parsing(example_graph):
    g: Graph = example_graph
    start_variants = [
        0,
        0.0,
        datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        # no timezone information, should assume UTC
        datetime(1970, 1, 1, 0, 0, 0),
        "1970-01-01T00:00:00Z",  # RFC3339
        "Thu, 01 Jan 1970 00:00:00 +0000",  # RFC2822
        "1970-01-01",  # date-only
        "1970-01-01T00:00:00.000",  # naive ISO T with ms
        "1970-01-01T00:00:00",  # naive ISO T
        "1970-01-01 00:00:00.000",  # naive space with ms
        "1970-01-01 00:00:00",  # naive space
        EventTime(0),
        EventTime(0, 0),
        # tuple/list indexed forms
        (0, 0),
        [0, 0],
        ("1970-01-01T00:00:00Z", 0),
        [datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 0],
        (0.0, 0),
    ]

    end_variants = [
        10,
        0.01,
        datetime(1970, 1, 1, 0, 0, 0, 10000, tzinfo=timezone.utc),
        # no timezone information, should assume UTC
        datetime(1970, 1, 1, 0, 0, 0, 10000),
        "1970-01-01T00:00:00.010Z",  # RFC3339 with ms
        "Thu, 01 Jan 1970 00:00:01 +0000",  # RFC2822 (1s)
        "1970-01-01T00:00:00.010",  # naive ISO T with ms
        "1970-01-01 00:00:00.010",  # naive space with ms
        EventTime(10),
        EventTime(10, 0),
        # tuple/list indexed forms
        (10, 0),
        [10, 0],
        ("1970-01-01T00:00:00.010Z", 0),
        [datetime(1970, 1, 1, 0, 0, 0, 10000, tzinfo=timezone.utc), 0],
        (0.01, 0),
    ]

    # Verify that every start form works when end is a simple int
    for start in start_variants:
        gw = g.window(start, 10)
        assert gw.nodes == [1], f"Unexpected nodes for start={start!r}"

    # Verify that every end form works when start is a simple int
    for end in end_variants:
        gw = g.window(0, end)
        assert gw.nodes == [1], f"Unexpected nodes for end={end!r}"

    assert g.window(86400000, 88200000).nodes == [2]
    assert g.window(86400000, 88200001).nodes == [2, 3]
    gw = g.window(88200000, "2000-01-01")
    assert gw.nodes == [3]
    gw = g.window(88200000, "2000-01-01 00:00:01")
    assert gw.nodes == [3, 4]
    gw = g.window(88200000, "2000-01-02")
    assert gw.nodes == [3, 4]

def test_optional_event_time_none_comparison():
    g = Graph()
    none_time = g.earliest_time

    assert none_time.is_none()
    assert not none_time

    assert none_time == None

    assert not (none_time > None)
    assert not (none_time < None)
    assert none_time >= None
    assert none_time <= None

    # None < Any EventTime/value
    assert none_time < 0
    assert none_time < 1000
    assert none_time < "1970-01-01"
    assert none_time < datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert none_time < EventTime(0)

    # Reverse comparisons
    assert 0 > none_time
    assert "1970-01-01" > none_time
    assert EventTime(0) > none_time

def test_optional_event_time_some_comparison():
    g = Graph()
    g.add_node(1000, 1)
    some_time = g.earliest_time

    assert some_time.is_some()
    assert some_time

    assert some_time != None
    assert some_time > None

    assert some_time == 1000
    assert some_time > 999
    assert some_time < 1001

    assert some_time == 1.0   # Floats are in seconds

    assert some_time == "1970-01-01 00:00:01"
    assert some_time > "1970-01-01 00:00:00"

    assert some_time == EventTime(1000, 0)
    assert some_time > EventTime(999, 0)

    assert some_time == (1000, 0)

def test_optional_vs_optional():
    g1 = Graph()
    none_time = g1.earliest_time

    g2 = Graph()
    g2.add_node(1000, 1)
    t1000 = g2.earliest_time

    g3 = Graph()
    g3.add_node(2000, 1)
    t2000 = g3.earliest_time

    assert none_time < t1000
    assert t1000 > none_time
    assert none_time != t1000

    assert t1000 < t2000
    assert t2000 > t1000
    assert t1000 != t2000

    g4 = Graph()
    g4.add_node(1000, 1)
    t1000_2 = g4.earliest_time
    assert t1000 == t1000_2