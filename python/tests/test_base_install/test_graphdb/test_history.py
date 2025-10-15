import pytest
from datetime import datetime, timezone
from raphtory import Graph, History, PersistentGraph


@pytest.fixture()
def example_graph() -> Graph:
    g: Graph = Graph()
    g.add_node(100, "Dumbledore")
    g.add_node(200, "Dumbledore", properties={"Age": 50})
    g.add_node(300, "Dumbledore", properties={"Age": 51})

    g.add_node(150, "Harry")
    g.add_node(250, "Harry", properties={"Age": 20})
    g.add_node(350, "Harry", properties={"Age": 21})

    g.add_edge(150, "Dumbledore", "Harry", layer="communication")
    g.add_edge(
        200, "Dumbledore", "Harry", properties={"weight": 0.5}, layer="friendship"
    )
    g.add_edge(
        300, "Dumbledore", "Harry", properties={"weight": 0.7}, layer="communication"
    )
    g.add_edge(
        350, "Dumbledore", "Harry", properties={"weight": 0.9}, layer="friendship"
    )
    return g


def test_node_and_edge_history_timestamps(example_graph):
    g: Graph = example_graph
    assert g.node("Dumbledore").history.t == [100, 150, 200, 200, 300, 300, 350]
    assert g.edge("Dumbledore", "Harry").history.t == [150, 200, 300, 350]

    assert g.window(0, 150).node("Dumbledore").history.t == [100]
    assert g.window(150, 300).node("Dumbledore").history.t == [150, 200, 200]
    assert g.window(300, 450).node("Dumbledore").history.t == [300, 300, 350]

    assert g.window(0, 150).edge("Dumbledore", "Harry") is None
    assert g.window(150, 300).edge("Dumbledore", "Harry").history.t == [150, 200]
    assert g.window(300, 450).edge("Dumbledore", "Harry").history.t == [300, 350]


def test_history_equality(example_graph):
    g: Graph = example_graph
    history = g.edge("Dumbledore", "Harry").history
    assert history.t == [150, 200, 300, 350]
    # compare with list of ints
    assert history == [150, 200, 300, 350]
    # compare with tuples
    assert history == [(150, 6), (200, 7), (300, 8), (350, 9)]
    # compare with floats
    assert history == [0.150, 0.2, 0.3, 0.35]
    # compare with datetime
    assert history == [
        datetime(1970, 1, 1, 0, 0, 0, 150_000),
        datetime(1970, 1, 1, 0, 0, 0, 200_000),
        datetime(1970, 1, 1, 0, 0, 0, 300_000),
        datetime(1970, 1, 1, 0, 0, 0, 350_000),
    ]
    # compare with tuples of datetime
    assert history == [
        (datetime(1970, 1, 1, 0, 0, 0, 150_000), 6),
        (datetime(1970, 1, 1, 0, 0, 0, 200_000), 7),
        (datetime(1970, 1, 1, 0, 0, 0, 300_000), 8),
        (datetime(1970, 1, 1, 0, 0, 0, 350_000), 9),
    ]
    # compare with datetime strings
    assert history == [
        "1970-01-01 00:00:00.150",
        "1970-01-01 00:00:00.200",
        "1970-01-01 00:00:00.300",
        "1970-01-01 00:00:00.350",
    ]
    # compare with tuples where event id is string
    assert history == [
        (datetime(1970, 1, 1, 0, 0, 0, 150_000), "1970-01-01 00:00:00.006"),
        (datetime(1970, 1, 1, 0, 0, 0, 200_000), "1970-01-01 00:00:00.007"),
        (datetime(1970, 1, 1, 0, 0, 0, 300_000), "1970-01-01 00:00:00.008"),
        (datetime(1970, 1, 1, 0, 0, 0, 350_000), "1970-01-01 00:00:00.009"),
    ]
    # compare mismatched input types
    assert history == [
        150,
        datetime(1970, 1, 1, 0, 0, 0, 200_000),
        "1970-01-01 00:00:00.300",
        "1970-01-01T00:00:00.350Z",
    ]
    # compare mismatched tuples
    assert history == [
        (150, 6),
        (datetime(1970, 1, 1, 0, 0, 0, 200_000), "1970-01-01 00:00:00.007"),
        ("1970-01-01 00:00:00.300", 0.008),
        ("1970-01-01T00:00:00.350Z", "1970-01-01T00:00:00.009"),
    ]


def test_layered_history_timestamps(example_graph):
    g: Graph = example_graph
    assert g.layer("friendship").node("Dumbledore").history.t == [
        100,
        200,
        200,
        300,
        350,
    ]
    assert g.layer("communication").edge("Dumbledore", "Harry").history.t == [150, 300]


def test_history_reverse_merge_and_compose(example_graph):
    g: Graph = example_graph
    node_history: History = g.node("Dumbledore").history
    edge_history: History = g.edge("Dumbledore", "Harry").history

    forward = node_history.t.collect().tolist()
    reverse = node_history.reverse().t.collect().tolist()
    assert reverse == list(reversed(forward))

    merged = node_history.merge(edge_history)
    assert merged.t.collect().tolist() == sorted(
        forward + edge_history.t.collect().tolist()
    )
    assert merged.t == [100, 150, 150, 200, 200, 200, 300, 300, 300, 350, 350]

    composed = History.compose_histories([node_history, edge_history])
    assert composed.t.collect().tolist() == merged.t.collect().tolist()


def test_intervals_basic_and_same_timestamp():
    g: Graph = Graph()
    g.add_node(1, "N")
    g.add_node(4, "N")
    g.add_node(10, "N")
    g.add_node(30, "N")
    assert g.node("N").history.intervals == [3, 6, 20]

    # Same timestamp intervals include 0
    g2 = Graph()
    g2.add_node(1, "X")
    g2.add_node(1, "X")
    assert g2.node("X").history.intervals == [0]
    g2.add_node(2, "X")
    assert g2.node("X").history.intervals == [0, 1]


def test_intervals_stats():
    g: Graph = Graph()
    g.add_node(1, "N")
    g.add_node(4, "N")
    g.add_node(10, "N")
    g.add_node(30, "N")
    interval_values = g.node("N").history.intervals
    assert interval_values.mean() == 29.0 / 3.0
    assert interval_values.median() == 6
    assert interval_values.max() == 20
    assert interval_values.min() == 3

    # No intervals if only one time entry
    g2: Graph = Graph()
    g2.add_node(1, "M")
    inter2 = g2.node("M").history.intervals
    assert inter2.mean() is None
    assert inter2.median() is None
    assert inter2.max() is None
    assert inter2.min() is None


def test_event_id_ordering_with_same_timestamp():
    g: Graph = Graph()

    g.add_node(1, "A", event_id=3)
    g.add_node(1, "A", event_id=2)
    g.add_node(1, "A", event_id=1)

    assert g.node("A").history.t == [1, 1, 1]

    # Event id should be ascending
    assert g.node("A").history.event_id == [1, 2, 3]
    assert g.node("A").history == [(1, 1), (1, 2), (1, 3)]


def test_composed_neighbours_history():
    g: Graph = Graph()
    g.add_node(1, "node")
    g.add_node(2, "node2")
    g.add_node(3, "node3")
    g.add_edge(4, "node", "node2")
    g.add_edge(5, "node", "node3")
    g.add_node(6, "node4")
    g.add_edge(7, "node2", "node4")

    # neighbours are node2 and node3
    neighbours = list(g.node("node").neighbours)
    neighbour_histories = [nb.history for nb in neighbours]
    combined = History.compose_histories(neighbour_histories)

    assert combined.earliest_time().t == 2
    assert combined.latest_time().t == 7
    # Expected: node2(2,4,7) + node3(3,5) in order
    assert combined == [(2, 1), (3, 2), (4, 3), (5, 4), (7, 6)]


def test_history_datetime_view():
    g: Graph = Graph()
    # Use datetimes and ensure dt view matches UTC conversions
    dt = datetime(2024, 1, 5, 12, 0, 0, tzinfo=timezone.utc)
    g.add_node(dt, "Z")
    g.add_node(dt.replace(hour=13), "Z")
    lst = g.node("Z").history.dt.collect()
    assert [d.tzinfo for d in lst] == [timezone.utc, timezone.utc]
    assert [d.hour for d in lst] == [12, 13]


def test_nodes_history_iterable(example_graph):
    g: Graph = example_graph
    names = g.nodes.name.collect()
    histories = [arr.collect().tolist() for arr in g.nodes.history.t.collect()]
    expected_by_node = {
        "Dumbledore": [100, 150, 200, 200, 300, 300, 350],
        "Harry": [150, 150, 200, 250, 300, 350, 350],
    }
    expected = [expected_by_node[n] for n in names]
    assert histories == expected


def test_node_states_intervals(example_graph):
    g: Graph = example_graph
    intervals = g.nodes.history.intervals
    assert intervals.collect() == [[50, 50, 0, 100, 0, 50], [0, 50, 50, 50, 50, 0]]
    assert intervals.mean() == [(50 + 50 + 100 + 50)/6, (50 + 50 + 50 + 50)/6]
    assert intervals.mean().min() == (50 + 50 + 50 + 50)/6
    assert intervals.median() == [50, 50]
    assert intervals.max() == [100, 50]
    assert intervals.min() == [0, 0]

    # the same functions should work on computed NodeStates
    intervals = intervals.compute()
    assert intervals.to_list() == [[50, 50, 0, 100, 0, 50], [0, 50, 50, 50, 50, 0]]
    assert intervals.mean() == [(50 + 50 + 100 + 50)/6, (50 + 50 + 50 + 50)/6]
    assert intervals.mean().min() == (50 + 50 + 50 + 50)/6
    assert intervals.median() == [50, 50]
    assert intervals.max() == [100, 50]
    assert intervals.min() == [0, 0]


def test_edges_history_iterable(example_graph):
    g: Graph = example_graph
    edges_histories = [arr.tolist() for arr in g.edges.history.t.collect()]
    assert edges_histories == [[150, 200, 300, 350]]


def test_nodes_neighbours_history_iterable(example_graph):
    g: Graph = example_graph

    neighbour_names = g.nodes.neighbours.name.collect()  # nested: List[List[str]]
    expected_by_node = {
        "Dumbledore": [100, 150, 200, 200, 300, 300, 350],
        "Harry": [150, 150, 200, 250, 300, 350, 350],
    }
    expected_nested = [
        [expected_by_node[n] for n in inner] for inner in neighbour_names
    ]
    nested_histories = [
        [arr.tolist() for arr in inner]
        for inner in g.nodes.neighbours.history.t.collect()
    ]
    assert nested_histories == expected_nested


def test_edge_deletions_history():
    g: PersistentGraph = PersistentGraph()
    g.add_edge(1, "A", "B")
    g.add_edge(2, "A", "C")
    g.delete_edge(3, "A", "B")
    g.delete_edge(4, "A", "C")

    # Per-edge deletions
    assert g.edge("A", "B").deletions.t == [3]
    assert g.edge("A", "C").deletions.t == [4]

    # Iterable of deletions across edges
    deletions_by_edge = [arr.tolist() for arr in g.edges.deletions.t.collect()]
    assert deletions_by_edge == [[3], [4]]
