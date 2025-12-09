import os
from pathlib import Path

import polars as pl
import pandas as pd
import pyarrow as pa
from raphtory import Graph, PersistentGraph, PropType
import pytest
try:
    import fireducks.pandas as fpd
except ModuleNotFoundError:
    fpd = None

def _collect_edges(g: Graph):
    return sorted((e.history()[0], e.src.id, e.dst.id, e["value"]) for e in g.edges)

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
    g_to_pandas.load_edges_from_pandas(df=df.to_pandas(), time="time", src="src", dst="dst", properties=["value"])

    g_from_df = graph_type()
    g_from_df.load_edges_from_df(data=df, time="time", src="src", dst="dst", properties=["value"])

    expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]
    assert _collect_edges(g_to_pandas) == _collect_edges(g_from_df)
    assert _collect_edges(g_to_pandas) == expected
    assert _collect_edges(g_from_df) == expected

def test_different_data_sources():
    num_nodes_ingested = []

    ######### PARQUET #########
    parquet_dir_path_str = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/parquet_subset"
    parquet_file_path_str = parquet_dir_path_str + "/flattened_data_subset.parquet"
    # test path string for parquet file
    g = Graph()
    g.load_nodes(data=parquet_file_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test Path object for parquet file
    file_path_obj = Path(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test path string for parquet directory
    g = Graph()
    g.load_nodes(data=parquet_dir_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test Path object for parquet directory
    dir_path_obj = Path(parquet_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    ######### CSV #########
    csv_dir_path_str = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/csv_subset"
    csv_file_path_str = csv_dir_path_str + "/flattened_data_subset.csv"
    # test path string for CSV file
    g = Graph()
    g.load_nodes(data=csv_file_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test Path object for CSV file
    file_path_obj = Path(csv_file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test path string for CSV directory
    g = Graph()
    g.load_nodes(data=csv_dir_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test Path object for CSV directory
    dir_path_obj = Path(csv_dir_path_str)
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    ######### arrow_c_stream #########
    # test pandas
    df_pd = pd.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pd, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, df_pd

    # test polars
    df_pl = pl.read_parquet(parquet_file_path_str)
    g = Graph()
    g.load_nodes(data=df_pl, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, df_pl

    # sanity check, make sure we ingested the same number of nodes each time
    print(f"Number of tests ran: {len(num_nodes_ingested)}")
    for i in range(len(num_nodes_ingested)-1):
        assert num_nodes_ingested[0] == num_nodes_ingested[i+1]

def test_schema_casting():
    # time/id as regular ints (I64), value column as explicit int32
    df = pd.DataFrame(
        {
            "time": pd.Series([1, 2, 3], dtype="int64"),
            "id": pd.Series([10, 20, 30], dtype="int64"),
            "val_i32": pd.Series([1, 2, 3], dtype="int32"),
        }
    )
    g = Graph()
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        # No casting
    )
    n_prop = g.node(10).properties
    print(f"\ndtype of Property 'val_i32' without cast:       {n_prop.get_dtype_of("val_i32")}")
    del g

    g = Graph()
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        # Request that this column be treated as I64
        schema=[("val_i32", PropType.i64)],
    )
    n_prop = g.node(10).properties
    print(f"dtype of Property 'val_i32' with PropType cast: {n_prop.get_dtype_of("val_i32")}")
    del g

    g = Graph()
    g.load_nodes(
        data=df,
        time="time",
        id="id",
        properties=["val_i32"],
        # Request that this column be treated as I64
        schema=[("val_i32", pa.int64())],
    )
    n_prop = g.node(10).properties
    print(f"dtype of Property 'val_i32' with pyarrow cast:  {n_prop.get_dtype_of("val_i32")}")


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
        g.load_edges_from_df(data=df, time="time", src="src", dst="dst", properties=["value"])
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
        g_fireducks.load_edges_from_df(data=df_fireducks, time="time", src="src", dst="dst", properties=["value"])

        g_pandas = graph_type()
        g_pandas.load_edges_from_pandas(df=df_pandas, time="time", src="src", dst="dst", properties=["value"])

        expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]

        assert _collect_edges(g_fireducks) == _collect_edges(g_pandas)
        assert _collect_edges(g_fireducks) == expected
        assert _collect_edges(g_pandas) == expected