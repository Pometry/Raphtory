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
    t = EventTime.new((5, 1))
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
    t2 = EventTime.new((1000, 1))
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
        EventTime.new(0),
        EventTime.new((0, 0)),
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
        EventTime.new(10),
        EventTime.new((10, 0)),
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
