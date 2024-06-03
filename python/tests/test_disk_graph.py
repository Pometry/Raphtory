from raphtory import DiskGraph, Query, State, PyDirection
import pandas as pd
import tempfile

edges = pd.DataFrame(
    {
        "src": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        "dst": [2, 3, 4, 5, 1, 3, 4, 5, 1, 2, 4, 5, 1, 2, 3, 5, 1, 2, 3, 4],
        "time": [
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
            110,
            120,
            130,
            140,
            150,
            160,
            170,
            180,
            190,
            200,
        ],
    }
).sort_values(["src", "dst", "time"])


def create_graph(edges, dir):
    return DiskGraph.load_from_pandas(dir, edges, "src", "dst", "time")


# in every test use with to create a temporary directory that will be deleted automatically
# after the with block ends


def test_counts():
    dir = tempfile.TemporaryDirectory()
    graph = create_graph(edges, dir.name)
    assert graph.count_nodes() == 5
    assert graph.count_edges() == 20


def test_simple_hop():
    dir = tempfile.TemporaryDirectory()
    graph = create_graph(edges, dir.name)
    q = Query.from_node_ids([1]).hop(dir=PyDirection("OUT"), layer=None, limit=100)
    state = State.path()
    actual = q.run_to_vec(graph, state)

    actual = [([n2.name, n1.name], n2.name) for ([n2, n1], n2) in actual]

    expected = [
        (["2", "1"], "2"),
        (["3", "1"], "3"),
        (["4", "1"], "4"),
        (["5", "1"], "5"),
    ]

    actual.sort()
    expected.sort()

    assert actual == expected


def test_simple_hop_from_node():
    dir = tempfile.TemporaryDirectory()
    graph = create_graph(edges, dir.name)
    node = graph.node(1)
    q = Query.from_node_ids([node]).out()
    state = State.path()
    actual = q.run_to_vec(graph, state)

    actual = [([n2.name, n1.name], n2.name) for ([n2, n1], n2) in actual]

    expected = [
        (["2", "1"], "2"),
        (["3", "1"], "3"),
        (["4", "1"], "4"),
        (["5", "1"], "5"),
    ]

    actual.sort()
    expected.sort()

    assert actual == expected


def test_double_hop():
    dir = tempfile.TemporaryDirectory()
    graph = create_graph(edges, dir.name)
    q = Query.from_node_ids([1]).out().out()
    state = State.path()
    actual = q.run_to_vec(graph, state)

    actual = [([n3.name, n2.name, n1.name], n3.name) for ([n3, n2, n1], n3) in actual]

    expected = [
        (["1", "5", "1"], "1"),
        (["2", "4", "1"], "2"),
        (["5", "3", "1"], "5"),
        (["2", "5", "1"], "2"),
        (["4", "2", "1"], "4"),
        (["4", "3", "1"], "4"),
        (["1", "4", "1"], "1"),
        (["3", "2", "1"], "3"),
        (["3", "4", "1"], "3"),
        (["5", "2", "1"], "5"),
        (["1", "2", "1"], "1"),
        (["5", "4", "1"], "5"),
        (["2", "3", "1"], "2"),
        (["1", "3", "1"], "1"),
        (["3", "5", "1"], "3"),
        (["4", "5", "1"], "4"),
    ]

    actual.sort()
    expected.sort()

    assert actual == expected


def test_hop_twice_forward():
    dir = tempfile.TemporaryDirectory()
    edges = pd.DataFrame(
        {
            "src": [0, 0, 1, 1, 3, 3, 3, 4, 4, 4],
            "dst": [1, 2, 3, 4, 5, 6, 6, 3, 4, 7],
            "time": [11, 10, 12, 13, 5, 10, 15, 14, 14, 10],
        }
    ).sort_values(["src", "dst", "time"])
    graph = create_graph(edges, dir.name)
    q = Query.from_node_ids([0, 1]).out().out()
    state = State.path_window(keep_path=True, start_t=10, duration=100)
    actual = q.run_to_vec(graph, state)

    actual = [([n3.name, n2.name, n1.name], n3.name) for ([n3, n2, n1], n3) in actual]

    expected = [
        (["6", "3", "1"], "6"),
        (["3", "1", "0"], "3"),
        (["3", "4", "1"], "3"),
        (["4", "4", "1"], "4"),
        (["4", "1", "0"], "4"),
    ]
    actual.sort()
    expected.sort()
    assert actual == expected
