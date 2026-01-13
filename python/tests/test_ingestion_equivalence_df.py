import os.path
import pytest
from pathlib import Path
import pandas as pd
import polars as pl
import pyarrow as pa
import duckdb

try:
    import fireducks.pandas as fpd
except ModuleNotFoundError:
    fpd = None
from raphtory import Graph, PersistentGraph

base_dir = Path(__file__).parent
EDGES_FILE = os.path.join(base_dir, "data/network_traffic_edges.csv")
NODES_FILE = os.path.join(base_dir, "data/network_traffic_nodes.csv")


def duck_query(con, sql: str):
    return con.execute(sql).arrow()


@pytest.fixture(scope="module")
def dataframes():
    # Load Data using Pandas
    df_edges_pd = pd.read_csv(EDGES_FILE)
    df_nodes_pd = pd.read_csv(NODES_FILE)

    con = duckdb.connect(database=":memory:")
    con.register("edges_df", df_edges_pd)
    con.register("nodes_df", df_nodes_pd)

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
    g_pd.load_edges_from_pandas(
        df=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )

    # Pandas streaming
    g_pd_stream = graph_type()
    g_pd_stream.load_edges_from_df(
        data=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
        properties=["data_size_MB", "transaction_type"],
        metadata=["is_encrypted"],
    )
    assert (
        g_pd == g_pd_stream
    ), "Pandas streaming edge ingestion failed equivalence check"

    # Polars
    g_pl = graph_type()
    g_pl.load_edges_from_df(
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
    g_arrow.load_edges_from_df(
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
    g_duckdb.load_edges_from_df(
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
        g_fd.load_edges_from_df(
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
    g_pd.load_nodes_from_pandas(
        df=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )

    # Pandas streaming
    g_pd_stream = graph_type()
    g_pd_stream.load_nodes_from_df(
        data=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    assert (
        g_pd == g_pd_stream
    ), "Pandas streaming node ingestion failed equivalence check"

    # Polars
    g_pl = graph_type()
    g_pl.load_nodes_from_df(
        data=dataframes["polars"]["nodes"],
        time="timestamp",
        id="server_id",
        properties=["OS_version", "uptime_days"],
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    assert g_pd == g_pl, "Polars node ingestion failed equivalence check"

    # Arrow
    g_arrow = graph_type()
    g_arrow.load_nodes_from_df(
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
    g_duckdb.load_nodes_from_df(
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
        g_fd.load_nodes_from_df(
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
    g_pd.load_edges_from_pandas(
        df=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_pd.load_nodes_from_pandas(
        df=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_pd.load_node_props_from_pandas(
        df=dataframes["pandas"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_pd.load_edge_props_from_pandas(
        df=dataframes["pandas"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )

    # Pandas streaming
    g_pd_stream = graph_type()
    g_pd_stream.load_edges_from_df(
        data=dataframes["pandas"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_pd_stream.load_nodes_from_df(
        data=dataframes["pandas"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_pd_stream.load_node_metadata_from_df(
        data=dataframes["pandas"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_pd_stream.load_edge_metadata_from_df(
        data=dataframes["pandas"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert (
        g_pd == g_pd_stream
    ), "Pandas streaming metadata ingestion failed equivalence check"

    # Polars
    g_pl = graph_type()
    g_pl.load_edges_from_df(
        data=dataframes["polars"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_pl.load_nodes_from_df(
        data=dataframes["polars"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_pl.load_node_metadata_from_df(
        data=dataframes["polars"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_pl.load_edge_metadata_from_df(
        data=dataframes["polars"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_pl, "Polars metadata ingestion failed equivalence check"

    # Arrow
    g_arrow = graph_type()
    g_arrow.load_edges_from_df(
        data=dataframes["arrow"]["edges"],
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_arrow.load_nodes_from_df(
        data=dataframes["arrow"]["nodes"],
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_arrow.load_node_metadata_from_df(
        data=dataframes["arrow"]["nodes"],
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_arrow.load_edge_metadata_from_df(
        data=dataframes["arrow"]["edges"],
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_arrow, "Arrow metadata ingestion failed equivalence check"

    # DuckDB
    g_duckdb = graph_type()
    con = dataframes["duckdb"]["con"]
    g_duckdb.load_edges_from_df(
        data=duck_query(con, "SELECT * FROM edges_df"),
        time="timestamp",
        src="source",
        dst="destination",
    )
    g_duckdb.load_nodes_from_df(
        data=duck_query(con, "SELECT * FROM nodes_df"),
        time="timestamp",
        id="server_id",
    )
    # update metadata
    g_duckdb.load_node_metadata_from_df(
        data=duck_query(con, "SELECT * FROM nodes_df"),
        id="server_id",
        metadata=["primary_function", "server_name", "hardware_type"],
    )
    g_duckdb.load_edge_metadata_from_df(
        data=duck_query(con, "SELECT * FROM edges_df"),
        src="source",
        dst="destination",
        metadata=["is_encrypted"],
    )
    assert g_pd == g_duckdb, "DuckDB metadata ingestion failed equivalence check"

    if fpd:
        # FireDucks
        g_fd = graph_type()
        g_fd.load_edges_from_df(
            data=dataframes["fireducks"]["edges"],
            time="timestamp",
            src="source",
            dst="destination",
        )
        g_fd.load_nodes_from_df(
            data=dataframes["fireducks"]["nodes"],
            time="timestamp",
            id="server_id",
        )
        # update metadata
        g_fd.load_node_metadata_from_df(
            data=dataframes["fireducks"]["nodes"],
            id="server_id",
            metadata=["primary_function", "server_name", "hardware_type"],
        )
        g_fd.load_edge_metadata_from_df(
            data=dataframes["fireducks"]["edges"],
            src="source",
            dst="destination",
            metadata=["is_encrypted"],
        )
        assert g_pd == g_fd, "FireDucks metadata ingestion failed equivalence check"
