import os
import tempfile

import pyarrow as pa
import pytest
from raphtory import Graph, PersistentGraph

# deltalake is an optional test dependency; skip the module if it is absent.
deltalake = pytest.importorskip("deltalake")
from deltalake import DeltaTable, write_deltalake


@pytest.fixture
def delta_dir():
    d = tempfile.TemporaryDirectory()
    yield d.name
    d.cleanup()


def _reader(dt):
    """Streaming ``pyarrow.RecordBatchReader`` (exposes ``__arrow_c_stream__``)
    for a Delta table handle, mirroring PyIceberg's ``to_arrow_batch_reader``."""
    return dt.to_pyarrow_dataset().scanner().to_reader()


def edge_tuples(g):
    return sorted(e.id for e in g.edges)


def test_load_from_delta(delta_dir):
    nodes_path = os.path.join(delta_dir, "nodes")
    edges_path = os.path.join(delta_dir, "edges")

    write_deltalake(
        nodes_path,
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                "name": pa.array(
                    ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"], pa.string()
                ),
                "time": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            }
        ),
    )
    write_deltalake(
        edges_path,
        pa.table(
            {
                "src": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "dst": pa.array([2, 3, 4, 5, 6], pa.int64()),
                "time": pa.array([1, 2, 3, 4, 5], pa.int64()),
                "weight": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
                "marbles": pa.array(
                    ["red", "blue", "green", "yellow", "purple"], pa.string()
                ),
            }
        ),
    )

    for graph in (Graph(), PersistentGraph()):
        graph.load_edges(
            _reader(DeltaTable(edges_path)),
            time="time",
            src="src",
            dst="dst",
            properties=["weight", "marbles"],
        )
        graph.load_nodes(
            _reader(DeltaTable(nodes_path)),
            time="time",
            id="id",
            properties=["name"],
        )

        assert graph.nodes.id.sorted() == [1, 2, 3, 4, 5, 6]
        edges = [(*e.id, e["weight"], e["marbles"]) for e in graph.edges]
        edges.sort()
        assert edges == [
            (1, 2, 1.0, "red"),
            (2, 3, 2.0, "blue"),
            (3, 4, 3.0, "green"),
            (4, 5, 4.0, "yellow"),
            (5, 6, 5.0, "purple"),
        ]


def test_delta_add_column(delta_dir):
    """A new column added in a later Delta version (schema merge) reads back as
    null for rows written before it existed; time-travel to v0 predates it."""
    path = os.path.join(delta_dir, "edges")

    write_deltalake(  # version 0
        path,
        pa.table(
            {
                "src": pa.array([1, 2, 3], pa.int64()),
                "dst": pa.array([2, 3, 4], pa.int64()),
                "time": pa.array([1, 2, 3], pa.int64()),
                "weight": pa.array([1.0, 2.0, 3.0], pa.float64()),
            }
        ),
    )
    write_deltalake(  # version 1: adds `colour`
        path,
        pa.table(
            {
                "src": pa.array([4], pa.int64()),
                "dst": pa.array([5], pa.int64()),
                "time": pa.array([4], pa.int64()),
                "weight": pa.array([4.0], pa.float64()),
                "colour": pa.array(["red"], pa.string()),
            }
        ),
        mode="append",
        schema_mode="merge",
    )

    g = Graph()
    g.load_edges(
        _reader(DeltaTable(path)),
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "colour"],
    )
    assert g.edge(4, 5)["colour"] == "red"
    assert g.edge(1, 2)["colour"] is None  # predates the column
    assert g.edge(1, 2)["weight"] == 1.0

    old = Graph()
    dt0 = DeltaTable(path)
    dt0.load_as_version(0)
    old.load_edges(
        _reader(dt0), time="time", src="src", dst="dst", properties=["weight"]
    )
    assert (4, 5) not in edge_tuples(old)  # the colour row did not exist at v0
    assert old.edge(1, 2)["weight"] == 1.0


def test_delta_change_column_type(delta_dir):
    """Changing a column's type across Delta versions (overwrite) is fine for a
    fresh load, time-travel preserves the old type, and feeding both versions
    into one graph is rejected because Raphtory properties are strongly typed."""
    path = os.path.join(delta_dir, "edges")

    write_deltalake(  # version 0: weight is int32
        path,
        pa.table(
            {
                "src": pa.array([1, 2], pa.int64()),
                "dst": pa.array([2, 3], pa.int64()),
                "time": pa.array([1, 2], pa.int64()),
                "weight": pa.array([10, 20], pa.int32()),
            }
        ),
    )
    assert DeltaTable(path).schema().to_arrow().field("weight").type == pa.int32()

    write_deltalake(  # version 1: weight widened to int64
        path,
        pa.table(
            {
                "src": pa.array([1, 2, 3], pa.int64()),
                "dst": pa.array([2, 3, 4], pa.int64()),
                "time": pa.array([1, 2, 3], pa.int64()),
                "weight": pa.array([10, 20, 30], pa.int64()),
            }
        ),
        mode="overwrite",
        schema_mode="overwrite",
    )
    assert DeltaTable(path).schema().to_arrow().field("weight").type == pa.int64()

    g = Graph()
    g.load_edges(
        _reader(DeltaTable(path)),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert g.edge(3, 4)["weight"] == 30
    assert g.edge(1, 2)["weight"] == 10

    old = Graph()
    dt0 = DeltaTable(path)
    dt0.load_as_version(0)
    old.load_edges(
        _reader(dt0), time="time", src="src", dst="dst", properties=["weight"]
    )
    assert old.edge(1, 2)["weight"] == 10

    # Same graph, both versions: int then long is a type conflict.
    mixed = Graph()
    dt0b = DeltaTable(path)
    dt0b.load_as_version(0)
    mixed.load_edges(
        _reader(dt0b), time="time", src="src", dst="dst", properties=["weight"]
    )
    with pytest.raises(Exception, match="Wrong type"):
        mixed.load_edges(
            _reader(DeltaTable(path)),
            time="time",
            src="src",
            dst="dst",
            properties=["weight"],
        )


def test_delta_incremental_cdf(delta_dir):
    """Change Data Feed returns only the rows changed since a given version,
    tagged by change type, so you can ingest just the diff: inserts feed
    load_edges and deletes feed load_edge_deletions on a PersistentGraph."""
    path = os.path.join(delta_dir, "edges")

    write_deltalake(  # version 0 (the baseline, assumed already ingested)
        path,
        pa.table(
            {
                "src": pa.array([1, 2], pa.int64()),
                "dst": pa.array([2, 3], pa.int64()),
                "time": pa.array([1, 2], pa.int64()),
                "weight": pa.array([1.0, 2.0], pa.float64()),
            }
        ),
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    write_deltalake(  # version 1: a new edge is appended
        path,
        pa.table(
            {
                "src": pa.array([3], pa.int64()),
                "dst": pa.array([4], pa.int64()),
                "time": pa.array([3], pa.int64()),
                "weight": pa.array([3.0], pa.float64()),
            }
        ),
        mode="append",
    )
    DeltaTable(path).delete("src = 1")  # version 2: edge (1, 2) is removed

    # The diff since version 1: exactly one insert and one delete, nothing else.
    changes = (
        pa.RecordBatchReader.from_stream(DeltaTable(path).load_cdf(starting_version=1))
        .read_all()
        .to_pandas()
    )
    inserts = changes[changes["_change_type"] == "insert"]
    deletes = changes[changes["_change_type"] == "delete"]
    assert sorted(zip(inserts["src"].tolist(), inserts["dst"].tolist())) == [(3, 4)]
    assert sorted(zip(deletes["src"].tolist(), deletes["dst"].tolist())) == [(1, 2)]

    # Seed a graph from the baseline, then apply only the diff.
    g = PersistentGraph()
    dt0 = DeltaTable(path)
    dt0.load_as_version(0)
    g.load_edges(_reader(dt0), time="time", src="src", dst="dst", properties=["weight"])
    assert sorted(e.id for e in g.edges) == [(1, 2), (2, 3)]

    g.load_edges(inserts, time="time", src="src", dst="dst", properties=["weight"])
    deletes = deletes.copy()
    deletes["time"] = 100  # apply the deletion after all existing events
    g.load_edge_deletions(deletes, time="time", src="src", dst="dst")

    # Before the deletion edge (1, 2) is live; after it, only the survivors remain.
    assert sorted(e.id for e in g.at(50).edges) == [(1, 2), (2, 3), (3, 4)]
    assert sorted(e.id for e in g.at(150).edges) == [(2, 3), (3, 4)]
