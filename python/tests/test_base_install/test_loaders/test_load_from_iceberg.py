import os
import tempfile

import pyarrow as pa
import pytest
from raphtory import Graph, PersistentGraph, PropType

# PyIceberg is an optional test dependency; skip the whole module if it (or its
# SQL-catalog extra) is not installed rather than erroring on collection.
pyiceberg = pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture(scope="session")
def iceberg_tables():
    """Build a local Iceberg warehouse backed by a SQLite catalog and populate
    it with a nodes table and an edges table.

    Yields the two loaded ``pyiceberg`` Table handles. Data mirrors the parquet
    loader fixtures so the assertions are directly comparable.
    """
    dirname = tempfile.TemporaryDirectory()
    warehouse = os.path.join(dirname.name, "warehouse")
    os.makedirs(warehouse, exist_ok=True)

    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{os.path.join(warehouse, 'catalog.db')}",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("raphtory")

    nodes = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            "name": pa.array(
                ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank"], pa.string()
            ),
            "time": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
        }
    )
    nodes_tbl = catalog.create_table("raphtory.nodes", schema=nodes.schema)
    nodes_tbl.append(nodes)

    edges = pa.table(
        {
            "src": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "dst": pa.array([2, 3, 4, 5, 6], pa.int64()),
            "time": pa.array([1, 2, 3, 4, 5], pa.int64()),
            "weight": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
            "marbles": pa.array(
                ["red", "blue", "green", "yellow", "purple"], pa.string()
            ),
        }
    )
    edges_tbl = catalog.create_table("raphtory.edges", schema=edges.schema)
    edges_tbl.append(edges)

    yield nodes_tbl, edges_tbl

    dirname.cleanup()


def assert_expected(g):
    expected_node_ids = [1, 2, 3, 4, 5, 6]
    expected_nodes = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
        (6, "Frank"),
    ]
    expected_edges = [
        (1, 2, 1.0, "red"),
        (2, 3, 2.0, "blue"),
        (3, 4, 3.0, "green"),
        (4, 5, 4.0, "yellow"),
        (5, 6, 5.0, "purple"),
    ]

    nodes = [(v.id, v["name"]) for v in g.nodes]
    nodes.sort()
    edges = [(*e.id, e["weight"], e["marbles"]) for e in g.edges]
    edges.sort()

    assert g.nodes.id.sorted() == expected_node_ids
    assert nodes == expected_nodes
    assert edges == expected_edges


def test_load_from_iceberg_batch_reader(iceberg_tables):
    """Stream an Iceberg scan into a graph via ``to_arrow_batch_reader()``.

    The reader is a ``pyarrow.RecordBatchReader`` exposing ``__arrow_c_stream__``,
    which is the streaming path Raphtory consumes without materialising the
    whole scan up front. A fresh scan is issued per load because the reader is
    single-use.
    """
    nodes_tbl, edges_tbl = iceberg_tables

    graph = Graph()
    graph.load_edges(
        edges_tbl.scan().to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
    )
    graph.load_nodes(
        nodes_tbl.scan().to_arrow_batch_reader(),
        time="time",
        id="id",
        properties=["name"],
    )
    assert_expected(graph)


def test_load_from_iceberg_to_arrow(iceberg_tables):
    """Same load using the collecting ``to_arrow()`` path (materialised
    ``pyarrow.Table``) to confirm both Arrow C-stream producers behave
    identically at ingest.
    """
    nodes_tbl, edges_tbl = iceberg_tables

    g = Graph()
    g.load_edges(
        edges_tbl.scan().to_arrow(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "marbles"],
    )
    g.load_nodes(
        nodes_tbl.scan().to_arrow(),
        time="time",
        id="id",
        properties=["name"],
    )
    assert_expected(g)


@pytest.fixture
def iceberg_catalog():
    """A fresh, empty Iceberg catalog per test for schema-evolution scenarios."""
    dirname = tempfile.TemporaryDirectory()
    warehouse = os.path.join(dirname.name, "warehouse")
    os.makedirs(warehouse, exist_ok=True)

    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{os.path.join(warehouse, 'catalog.db')}",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("raphtory")

    yield catalog

    dirname.cleanup()


def edge_tuples(g):
    return sorted(e.id for e in g.edges)


def test_iceberg_add_column(iceberg_catalog):
    """A column added in a later Iceberg snapshot reads back as null for rows
    written before it existed; time-travel to the old snapshot predates it."""
    from pyiceberg.types import StringType

    v0 = pa.table(
        {
            "src": pa.array([1, 2, 3], pa.int64()),
            "dst": pa.array([2, 3, 4], pa.int64()),
            "time": pa.array([1, 2, 3], pa.int64()),
            "weight": pa.array([1.0, 2.0, 3.0], pa.float64()),
        }
    )
    tbl = iceberg_catalog.create_table("raphtory.edges", schema=v0.schema)
    tbl.append(v0)
    snapshot_v0 = tbl.current_snapshot().snapshot_id

    with tbl.update_schema() as update:
        update.add_column("colour", StringType())
    tbl.append(
        pa.table(
            {
                "src": pa.array([4], pa.int64()),
                "dst": pa.array([5], pa.int64()),
                "time": pa.array([4], pa.int64()),
                "weight": pa.array([4.0], pa.float64()),
                "colour": pa.array(["red"], pa.string()),
            }
        )
    )

    g = Graph()
    g.load_edges(
        tbl.scan().to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight", "colour"],
    )
    assert g.edge(4, 5)["colour"] == "red"
    assert g.edge(1, 2)["colour"] is None  # predates the column
    assert g.edge(1, 2)["weight"] == 1.0

    old = Graph()
    old.load_edges(
        tbl.scan(snapshot_id=snapshot_v0).to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert (4, 5) not in edge_tuples(old)  # the colour row did not exist at v0
    assert old.edge(1, 2)["weight"] == 1.0


def test_iceberg_change_column_type(iceberg_catalog):
    """Promoting a column's type across Iceberg snapshots (int -> long) is fine
    for a fresh load, time-travel preserves the old type, and feeding both
    snapshots into one graph is rejected by Raphtory's typed properties."""
    from pyiceberg.types import LongType

    v0 = pa.table(
        {
            "src": pa.array([1, 2], pa.int64()),
            "dst": pa.array([2, 3], pa.int64()),
            "time": pa.array([1, 2], pa.int64()),
            "weight": pa.array([10, 20], pa.int32()),  # Iceberg `int` (32-bit)
        }
    )
    tbl = iceberg_catalog.create_table("raphtory.edges", schema=v0.schema)
    tbl.append(v0)
    snapshot_v0 = tbl.current_snapshot().snapshot_id
    assert tbl.scan().to_arrow().schema.field("weight").type == pa.int32()

    with tbl.update_schema() as update:
        update.update_column("weight", field_type=LongType())  # int -> long
    tbl.append(
        pa.table(
            {
                "src": pa.array([3], pa.int64()),
                "dst": pa.array([4], pa.int64()),
                "time": pa.array([3], pa.int64()),
                "weight": pa.array([30], pa.int64()),
            }
        )
    )
    assert tbl.scan().to_arrow().schema.field("weight").type == pa.int64()

    g = Graph()
    g.load_edges(
        tbl.scan().to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert g.edge(3, 4)["weight"] == 30
    assert g.edge(1, 2)["weight"] == 10

    old = Graph()
    old.load_edges(
        tbl.scan(snapshot_id=snapshot_v0).to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert old.edge(1, 2)["weight"] == 10

    # Same graph, both snapshots: int then long is a type conflict.
    mixed = Graph()
    mixed.load_edges(
        tbl.scan(snapshot_id=snapshot_v0).to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    with pytest.raises(Exception, match="Wrong type"):
        mixed.load_edges(
            tbl.scan().to_arrow_batch_reader(),
            time="time",
            src="src",
            dst="dst",
            properties=["weight"],
        )


def test_iceberg_schema_reconciles_type_change(iceberg_catalog):
    """The loader's `schema` option casts columns on ingest, which resolves the
    cross-version type conflict above: casting both the pre- and post-promotion
    snapshots to i64 lets them load into a single graph."""
    from pyiceberg.types import LongType

    v0 = pa.table(
        {
            "src": pa.array([1], pa.int64()),
            "dst": pa.array([2], pa.int64()),
            "time": pa.array([1], pa.int64()),
            "weight": pa.array([10], pa.int32()),
        }
    )
    tbl = iceberg_catalog.create_table("raphtory.edges", schema=v0.schema)
    tbl.append(v0)
    snapshot_v0 = tbl.current_snapshot().snapshot_id

    with tbl.update_schema() as update:
        update.update_column("weight", field_type=LongType())
    tbl.append(
        pa.table(
            {
                "src": pa.array([1], pa.int64()),
                "dst": pa.array([2], pa.int64()),
                "time": pa.array([2], pa.int64()),
                "weight": pa.array([20], pa.int64()),
            }
        )
    )

    g = Graph()
    # Force both loads to the same property type instead of inferring per batch.
    g.load_edges(
        tbl.scan(snapshot_id=snapshot_v0).to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
        schema={"weight": PropType.i64()},
    )
    g.load_edges(
        tbl.scan().to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
        schema={"weight": PropType.i64()},
    )

    assert g.edge(1, 2).properties.get_dtype_of("weight") == PropType.i64()
    assert g.edge(1, 2)["weight"] == 20  # latest value across both snapshots


def test_iceberg_incremental_by_watermark(iceberg_catalog):
    """PyIceberg exposes no incremental-scan API, but append-only temporal data
    can be loaded incrementally with a row filter on the time column: only rows
    past the last-ingested watermark are scanned (Iceberg prunes files by their
    min/max stats), so already-loaded rows are not re-read."""
    from pyiceberg.expressions import GreaterThan

    tbl = iceberg_catalog.create_table(
        "raphtory.edges",
        schema=pa.schema(
            [
                ("src", pa.int64()),
                ("dst", pa.int64()),
                ("time", pa.int64()),
                ("weight", pa.float64()),
            ]
        ),
    )
    tbl.append(
        pa.table(
            {
                "src": pa.array([1, 2], pa.int64()),
                "dst": pa.array([2, 3], pa.int64()),
                "time": pa.array([1, 2], pa.int64()),
                "weight": pa.array([1.0, 2.0], pa.float64()),
            }
        )
    )

    g = Graph()
    g.load_edges(
        tbl.scan().to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert sorted(e.id for e in g.edges) == [(1, 2), (2, 3)]
    watermark = 2  # in practice: persist the max `time` ingested so far

    # New rows land in a later snapshot.
    tbl.append(
        pa.table(
            {
                "src": pa.array([3, 4], pa.int64()),
                "dst": pa.array([4, 5], pa.int64()),
                "time": pa.array([3, 4], pa.int64()),
                "weight": pa.array([3.0, 4.0], pa.float64()),
            }
        )
    )

    # The filtered scan returns only rows past the watermark, not the whole table.
    assert sorted(
        tbl.scan(row_filter=GreaterThan("time", watermark))
        .to_arrow()["time"]
        .to_pylist()
    ) == [3, 4]

    g.load_edges(
        tbl.scan(row_filter=GreaterThan("time", watermark)).to_arrow_batch_reader(),
        time="time",
        src="src",
        dst="dst",
        properties=["weight"],
    )
    assert sorted(e.id for e in g.edges) == [(1, 2), (2, 3), (3, 4), (4, 5)]
