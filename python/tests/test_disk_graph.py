from raphtory import PyDirection, DiskGraphStorage
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
    return DiskGraphStorage.load_from_pandas(dir, edges, "src", "dst", "time")


# in every test use with to create a temporary directory that will be deleted automatically
# after the with block ends


def test_counts():
    dir = tempfile.TemporaryDirectory()
    graph = create_graph(edges, dir.name).to_events()
    assert graph.count_nodes() == 5
    assert graph.count_edges() == 20

