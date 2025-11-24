import gc
import time

import pandas as pd
import polars as pl
import duckdb
import fireducks.pandas as fpd

from raphtory import Graph

FLATTENED_FILE = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/flattened_data.parquet"

def bench_pandas(df: pd.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_pandas(df=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[pandas]                ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_pandas_streaming(df: pd.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_df(data=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[pandas streaming]      ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_fire_ducks_pandas_streaming(df: fpd.frame.DataFrame) -> float:
    assert "fireducks.pandas.frame.DataFrame" in str(type(df))
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_df(data=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[fireducks streaming]   ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_polars_streaming(df: pl.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    g.load_edges(data_source=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[polars streaming]      ingestion took {total:.3f}s")
    del g
    gc.collect()
    return total

def bench_arrow_streaming(df: pl.DataFrame) -> float:
    g = Graph()
    df_arrow_from_pl = df.to_arrow()
    start = time.perf_counter()
    g.load_edges(data_source=df_arrow_from_pl, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[arrow streaming]       ingestion took {total:.3f}s")
    del g, df_arrow_from_pl
    gc.collect()
    return total

def bench_duckdb_streaming(df: pl.DataFrame) -> float:
    g = Graph()
    df_arrow_from_pl = df.to_arrow()
    duckdb_df = duckdb.sql("SELECT * FROM df_arrow_from_pl")
    start = time.perf_counter()
    # uses the __arrow_c_stream__() interface internally
    g.load_edges_from_df(data=duckdb_df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[duckdb streaming]      ingestion took {total:.3f}s")
    del g, df_arrow_from_pl, duckdb_df
    gc.collect()
    return total


def ingestion_speed_btc_dataset():
    df_pd: pd.DataFrame = pd.read_parquet(FLATTENED_FILE)
    df_fireducks: fpd.frame.DataFrame = fpd.read_parquet(FLATTENED_FILE)
    df_pl: pl.DataFrame = pl.read_parquet(FLATTENED_FILE)

    pandas_ingestion_times = []
    pandas_streaming_ingestion_times = []
    # fireducks_ingestion_times = []
    fireducks_streaming_ingestion_times = []
    # polars_ingestion_times = []
    polars_streaming_ingestion_times = []
    # arrow_ingestion_times = []
    arrow_streaming_ingestion_times = []
    # duckdb_ingestion_times = []
    duckdb_streaming_ingestion_times = []

    for _ in range(5):
        # 1.1) Pandas ingestion
        pandas_time = bench_pandas(df_pd)
        pandas_ingestion_times.append(pandas_time)
        gc.collect()

        # 1.2) Pandas ingestion streaming
        pandas_streaming_time = bench_pandas_streaming(df_pd)
        pandas_streaming_ingestion_times.append(pandas_streaming_time)
        gc.collect()

        # 2) Fireducks Pandas ingestion streaming
        fpd_streaming_time = bench_fire_ducks_pandas_streaming(df_fireducks)
        fireducks_streaming_ingestion_times.append(fpd_streaming_time)
        gc.collect()

        # 3) Polars ingestion streaming (no internal to_pandas() call)
        polars_streaming_time = bench_polars_streaming(df=df_pl)
        polars_streaming_ingestion_times.append(polars_streaming_time)
        gc.collect()

        # 4) Arrow ingestion streaming
        arrow_streaming_time = bench_arrow_streaming(df_pl)
        arrow_streaming_ingestion_times.append(arrow_streaming_time)
        gc.collect()

        # 5) DuckDB streaming ingestion (no internal fetch_arrow_table() call)
        duckdb_streaming_time = bench_duckdb_streaming(df_pl)
        duckdb_streaming_ingestion_times.append(duckdb_streaming_time)
        gc.collect()

    formatted_pandas = [f"{num:.3f}s" for num in pandas_ingestion_times]
    formatted_pandas_streaming = [f"{num:.3f}s" for num in pandas_streaming_ingestion_times]
    # formatted_fireducks = [f"{num:.3f}s" for num in fireducks_ingestion_times]
    formatted_fireducks_streaming = [f"{num:.3f}s" for num in fireducks_streaming_ingestion_times]
    # formatted_polars = [f"{num:.3f}s" for num in polars_ingestion_times]
    formatted_polars_streaming = [f"{num:.3f}s" for num in polars_streaming_ingestion_times]
    # formatted_arrow = [f"{num:.3f}s" for num in arrow_ingestion_times]
    formatted_arrow_streaming = [f"{num:.3f}s" for num in arrow_streaming_ingestion_times]
    # formatted_duckdb = [f"{num:.3f}s" for num in duckdb_ingestion_times]
    formatted_duckdb_streaming = [f"{num:.3f}s" for num in duckdb_streaming_ingestion_times]

    print(f"Pandas:              {formatted_pandas}")
    print(f"Pandas streaming:    {formatted_pandas_streaming}")
    # print(f"Fireducks:           {formatted_fireducks}")
    print(f"Fireducks streaming: {formatted_fireducks_streaming}")
    # print(f"Polars:              {formatted_polars}")
    print(f"Polars streaming:    {formatted_polars_streaming}")
    # print(f"Arrow:               {formatted_arrow}")
    print(f"Arrow streaming:     {formatted_arrow_streaming}")
    # print(f"DuckDB:              {formatted_duckdb}")
    print(f"DuckDB streaming:    {formatted_duckdb_streaming}")


if __name__ == "__main__":
    ingestion_speed_btc_dataset()