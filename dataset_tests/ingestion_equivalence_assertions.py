import gc

import duckdb

from raphtory import Graph
import pandas as pd
import polars as pl
import fireducks.pandas as fpd

FLATTENED_FILE = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/flattened_data.parquet"

if __name__ == "__main__":
    df_pd = pd.read_parquet(FLATTENED_FILE)
    g_pandas = Graph()
    g_pandas.load_edges_from_pandas(
        df=df_pd, time="block_timestamp", src="inputs_address", dst="outputs_address"
    )

    df_fireducks: fpd.frame.DataFrame = fpd.read_parquet(FLATTENED_FILE)
    g_fireducks = Graph()
    g_fireducks.load_edges_from_fireducks(df=df_fireducks, time="block_timestamp", src="inputs_address", dst="outputs_address")
    print("Checking equality...")
    assert g_pandas == g_fireducks
    print("g_pandas == g_fireducks")
    del df_fireducks, g_fireducks
    gc.collect()

    df_pl = pl.read_parquet(FLATTENED_FILE)
    g_polars = Graph()
    g_polars.load_edges_from_polars(
        df=df_pl, time="block_timestamp", src="inputs_address", dst="outputs_address"
    )

    print("Checking equality...")
    assert g_pandas == g_polars
    print("g_pandas == g_polars")

    df_pl_arrow = df_pl.to_arrow()
    g_polars_arrow = Graph()
    g_polars_arrow.load_edges_from_arrow(df=df_pl_arrow, time="block_timestamp", src="inputs_address", dst="outputs_address")
    print("Checking equality...")
    assert g_pandas == g_polars_arrow
    print("g_pandas == g_polars_arrow")
    del g_polars_arrow
    gc.collect()

    g_polars_arrow_streaming = Graph()
    g_polars_arrow_streaming.load_edges_from_arrow_streaming(df=df_pl_arrow, time="block_timestamp", src="inputs_address", dst="outputs_address")
    print("Checking equality...")
    assert g_pandas == g_polars_arrow_streaming
    print("g_pandas == g_polars_arrow_streaming")
    del g_polars_arrow_streaming
    gc.collect()

    g_duckdb = Graph()
    duckdb_results = duckdb.sql("SELECT * FROM df_pl_arrow")
    g_duckdb.load_edges_from_duckdb(df=duckdb_results, time="block_timestamp", src="inputs_address", dst="outputs_address")
    print("Checking equality...")
    assert g_pandas == g_duckdb
    print("g_pandas == g_duckdb")

    g_duckdb_streaming = Graph()
    g_duckdb_streaming.load_edges_from_duckdb_streaming(df=duckdb_results, time="block_timestamp", src="inputs_address", dst="outputs_address")
    print("Checking equality...")
    assert g_pandas == g_duckdb_streaming
    print("g_pandas == g_duckdb_streaming")