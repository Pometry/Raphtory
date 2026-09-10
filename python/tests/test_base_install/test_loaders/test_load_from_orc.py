import os
import tempfile

import pyarrow as pa
import pytest
from raphtory import Graph, PersistentGraph

# ORC support ships with pyarrow but requires the ORC-enabled build.
orc = pytest.importorskip("pyarrow.orc")


@pytest.fixture
def orc_files():
    d = tempfile.TemporaryDirectory()
    nodes_path = os.path.join(d.name, "nodes.orc")
    edges_path = os.path.join(d.name, "edges.orc")

    orc.write_table(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                "name": pa.array(
                    ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"], pa.string()
                ),
                "time": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            }
        ),
        nodes_path,
    )
    orc.write_table(
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
        edges_path,
    )

    yield nodes_path, edges_path

    d.cleanup()


def test_load_from_orc(orc_files):
    nodes_path, edges_path = orc_files

    for graph in (Graph(), PersistentGraph()):
        graph.load_edges(
            orc.read_table(edges_path),
            time="time",
            src="src",
            dst="dst",
            properties=["weight", "marbles"],
        )
        graph.load_nodes(
            orc.read_table(nodes_path),
            time="time",
            id="id",
            properties=["name"],
        )

        assert graph.nodes.id.sorted() == [1, 2, 3, 4, 5, 6]
        nodes = [(v.id, v.properties.get("name")) for v in graph.nodes]
        nodes.sort()
        assert nodes == [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carol"),
            (4, "Dave"),
            (5, "Eve"),
            (6, "Frank"),
        ]
        edges = [
            (*e.id, e.properties.get("weight"), e.properties.get("marbles"))
            for e in graph.edges
        ]
        edges.sort()
        assert edges == [
            (1, 2, 1.0, "red"),
            (2, 3, 2.0, "blue"),
            (3, 4, 3.0, "green"),
            (4, 5, 4.0, "yellow"),
            (5, 6, 5.0, "purple"),
        ]
