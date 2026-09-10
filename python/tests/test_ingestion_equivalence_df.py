import os.path
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

try:
    import fireducks.pandas as fpd
except ModuleNotFoundError:
    fpd = None
from raphtory import Graph, PersistentGraph

base_dir = Path(__file__).parent
EDGES_FILE = os.path.join(base_dir, "data/network_traffic_edges.csv")
NODES_FILE = os.path.join(base_dir, "data/network_traffic_nodes.csv")


def _btc_root() -> Path:
    return Path(__file__).parent / "data" / "btc_dataset"


def _collect_edges(g: Graph):
    return sorted(
        (e.history.t[0], e.src.id, e.dst.id, e.properties.get("value")) for e in g.edges
    )


def duck_query(con, sql: str):
    return con.execute(sql).arrow()


@pytest.fixture(scope="module")
def dataframes():
    # Load Data using Pandas
    df_edges_pd = pd.read_csv(EDGES_FILE)
    df_nodes_pd = pd.read_csv(NODES_FILE)

    con = duckdb.connect(database=":memory:")
    con.read_csv(EDGES_FILE).create("edges_df")
    con.read_csv(NODES_FILE).create("nodes_df")

    data = {
        "pandas": {"edges": df_edges_pd, "nodes": df_nodes_pd},
        "polars": {
            "edges": pl.from_pandas(df_edges_pd),
            "nodes": pl.from_pandas(df_nodes_pd),
        },
        "arrow": {
            "edges": pa.Table.from_pandas(df_edges_pd),
            "nodes": pa.Table.from_pandas(df_nodes_pd),
        },
        "duckdb": {"con": con},
    }
    if fpd:
        data["fireducks"] = {
            "edges": fpd.read_csv(EDGES_FILE),
            "nodes": fpd.read_csv(NODES_FILE),
        }

    return data


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_edge_ingestion_equivalence(dataframes, graph_type):
    # reference graph
    g_pd = graph_type()
    g_pd.load_edges(
        data=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )

    # Polars
    g_pl = graph_type()
    g_pl.load_edges(
        data=dataframes["polars"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )
    assert g_pd == g_pl, "Polars edge ingestion failed equivalence check"

    # Arrow
    g_arrow = graph_type()
    g_arrow.load_edges(
        data=dataframes["arrow"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )
    assert g_pd == g_arrow, "Arrow edge ingestion failed equivalence check"

    # DuckDB
    g_duckdb = graph_type()
    con = dataframes["duckdb"]["con"]
    g_duckdb.load_edges(
        data=duck_query(con, "SELECT * FROM edges_df"),
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )
    assert g_pd == g_duckdb, "DuckDB edge ingestion failed equivalence check"

    if fpd:
        # FireDucks
        g_fd = graph_type()
        g_fd.load_edges(
            data=dataframes["fireducks"]["edges"],
            time="timestamp",
            src="source",
            dst="destination",
            properties=["data_size_MB", "transaction_type"],
            metadata=["is_encrypted"],
        )
        assert g_pd == g_fd, "FireDucks edge ingestion failed equivalence check"


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_node_ingestion_equivalence(dataframes, graph_type):
    # reference graph
    g_pd = graph_type()
    g_pd.load_nodes(
        data=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )

    # Polars
    g_pl = graph_type()
    g_pl.load_nodes(
        data=dataframes["polars"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    assert g_pd == g_pl, "Polars node ingestion failed equivalence check"

    # Arrow
    g_arrow = graph_type()
    g_arrow.load_nodes(
        data=dataframes["arrow"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    assert g_pd == g_arrow, "Arrow node ingestion failed equivalence check"

    # DuckDB
    g_duckdb = graph_type()
    con = dataframes["duckdb"]["con"]
    g_duckdb.load_nodes(
        data=duck_query(con, "SELECT * FROM nodes_df"),
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    assert g_pd == g_duckdb, "DuckDB node ingestion failed equivalence check"

    if fpd:
        # FireDucks
        print("Testing fireducks...")
        g_fd = graph_type()
        g_fd.load_nodes(
            data=dataframes["fireducks"]["nodes"],
            time="timestamp",
            id="server_id",
            properties=["OS_version", "uptime_days"],
            metadata=["primary_function", "server_name", "hardware_type"],
        )
        assert g_pd == g_fd, "FireDucks node ingestion failed equivalence check"


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_metadata_update_equivalence(dataframes, graph_type):
    # reference graph
    g_pd = graph_type()
    g_pd.load_edges(
        data=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_pd.load_nodes(
        data=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_pd.load_node_metadata(
        data=dataframes["pandas"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_pd.load_edge_metadata(
        data=dataframes["pandas"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )

    # Polars
    g_pl = graph_type()
    g_pl.load_edges(
        data=dataframes["polars"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_pl.load_nodes(
        data=dataframes["polars"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_pl.load_node_metadata(
        data=dataframes["polars"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_pl.load_edge_metadata(
        data=dataframes["polars"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_pl, "Polars metadata ingestion failed equivalence check"

    # Arrow
    g_arrow = graph_type()
    g_arrow.load_edges(
        data=dataframes["arrow"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_arrow.load_nodes(
        data=dataframes["arrow"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_arrow.load_node_metadata(
        data=dataframes["arrow"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_arrow.load_edge_metadata(
        data=dataframes["arrow"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_arrow, "Arrow metadata ingestion failed equivalence check"

    # DuckDB
    g_duckdb = graph_type()
    con = dataframes["duckdb"]["con"]
    g_duckdb.load_edges(
        data=duck_query(con, "SELECT * FROM edges_df"),
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_duckdb.load_nodes(
        data=duck_query(con, "SELECT * FROM nodes_df"),
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_duckdb.load_node_metadata(
        data=duck_query(con, "SELECT * FROM nodes_df"),
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_duckdb.load_edge_metadata(
        data=duck_query(con, "SELECT * FROM edges_df"),
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_duckdb, "DuckDB metadata ingestion failed equivalence check"

    if fpd:
        # FireDucks
        g_fd = graph_type()
        g_fd.load_edges(
            data=dataframes["fireducks"]["edges"],
            time="timestamp",
            src="source",
            dst="destination",
        )
        g_fd.load_nodes(
            data=dataframes["fireducks"]["nodes"],
            time="timestamp",
            id="server_id",
        )
        # update metadata
        g_fd.load_node_metadata(
            data=dataframes["fireducks"]["nodes"],
            id="server_id",
            metadata=["primary_function", "server_name", "hardware_type"],
        )
        g_fd.load_edge_metadata(
            data=dataframes["fireducks"]["edges"],
            src="source",
            dst="destination",
            metadata=["is_encrypted"],
        )
        assert g_pd == g_fd, "FireDucks metadata ingestion failed equivalence check"


def test_different_data_sources():
    nodes_list = []

    ######### PARQUET #########
    parquet_dir_path_str = str(_btc_root() / "parquet_directory")
    parquet_file_path_str = str(_btc_root() / "flattened_data.parquet")
    # test path string for parquet file
    g = Graph()
    g.load_nodes(
        data=parquet_file_path_str, time="block_timestamp", id="inputs_address"
    )
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for parquet file
    file_path_obj = Path(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for parquet directory
    g = Graph()
    g.load_nodes(data=parquet_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for parquet directory
    dir_path_obj = Path(parquet_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### CSV #########
    csv_dir_path_str = str(_btc_root() / "csv_directory")
    csv_file_path_str = str(_btc_root() / "flattened_data.csv")
    # test path string for CSV file
    g = Graph()
    g.load_nodes(data=csv_file_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for CSV file
    file_path_obj = Path(csv_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for bz2 compressed CSV file
    g = Graph()
    compressed_file_path = csv_file_path_str + ".bz2"
    g.load_nodes(data=compressed_file_path, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for bz2 compressed CSV file
    file_path_obj = Path(compressed_file_path)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for gzip compressed CSV file
    g = Graph()
    compressed_file_path = csv_file_path_str + ".gz"
    g.load_nodes(data=compressed_file_path, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for gzip compressed CSV file
    file_path_obj = Path(compressed_file_path)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test path string for CSV directory
    g = Graph()
    g.load_nodes(data=csv_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object for CSV directory
    dir_path_obj = Path(csv_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### mixed directory #########
    mixed_dir_path_str = (
        str(Path(__file__).parent) + "/data/btc_dataset/mixed_directory"
    )
    # test path string
    g = Graph()
    g.load_nodes(data=mixed_dir_path_str, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    # test Path object
    g = Graph()
    g.load_nodes(
        data=Path(mixed_dir_path_str), time="block_timestamp", id="inputs_address"
    )
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g

    ######### arrow_c_stream #########
    # test pandas
    df_pd = pd.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pd, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g, df_pd

    # test polars
    df_pl = pl.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pl, time="block_timestamp", id="inputs_address")
    nodes_list.append(sorted(g.nodes.id.collect()))
    del g, df_pl

    # sanity check, make sure we ingested the same nodes each time
    print(f"Number of tests ran: {len(nodes_list)}")
    for i in range(len(nodes_list) - 1):
        assert (
            nodes_list[0] == nodes_list[i + 1]
        ), f"Nodes list assertion failed at item i={i}"


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_load_edges_from_polars_df(graph_type):
    df = pl.DataFrame(
        {
            "time": [1, 2, 3],
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
            "value": [10.0, 20.0, 30.0],
        }
    )

    g_to_pandas = graph_type()
    g_to_pandas.load_edges(
        data=df.to_pandas(), time="time", src="src", dst="dst", properties=["value"]
    )

    g_from_df = graph_type()
    g_from_df.load_edges(
        data=df, time="time", src="src", dst="dst", properties=["value"]
    )

    expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]
    assert _collect_edges(g_to_pandas) == _collect_edges(g_from_df)
    assert _collect_edges(g_to_pandas) == expected
    assert _collect_edges(g_from_df) == expected


if fpd:
    import pandas

    @pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
    def test_load_edges_from_fireducks_df(graph_type):
        # FireDucks DataFrame (pandas-compatible API)
        df = fpd.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )

        g = graph_type()
        g.load_edges(data=df, time="time", src="src", dst="dst", properties=["value"])
        assert [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)] == _collect_edges(g)

    @pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
    def test_fireducks_matches_pandas_for_same_edges(graph_type):
        df_fireducks = fpd.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )
        df_pandas = pandas.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )

        g_fireducks = graph_type()
        g_fireducks.load_edges(
            data=df_fireducks, time="time", src="src", dst="dst", properties=["value"]
        )

        g_pandas = graph_type()
        g_pandas.load_edges(
            data=df_pandas, time="time", src="src", dst="dst", properties=["value"]
        )

        expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]

        assert _collect_edges(g_fireducks) == _collect_edges(g_pandas)
        assert _collect_edges(g_fireducks) == expected
        assert _collect_edges(g_pandas) == expected
