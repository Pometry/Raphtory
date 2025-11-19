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
    print(f"[pandas] ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_fire_ducks_pandas(df: fpd.frame.DataFrame) -> float:
    assert "fireducks.pandas.frame.DataFrame" in str(type(df))
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_fireducks(df=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[fireducks] ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_fire_ducks_pandas_streaming(df: fpd.frame.DataFrame) -> float:
    assert "fireducks.pandas.frame.DataFrame" in str(type(df))
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_fireducks(df=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(f"[fireducks] streaming ingestion took {total:.3f}s for {len(df)} rows, edges: {len(g.edges)}, exploded edges: {len(g.edges.explode())}")
    del g
    gc.collect()
    return total

def bench_polars_native(df: pl.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    g.load_edges_from_polars(df=df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    total = time.perf_counter() - start
    print(
        f"[polars native] ingestion took {total:.3f}s"
    )
    del g
    gc.collect()
    return total

def bench_polars_to_arrow(df: pl.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    df_arrow_from_pl = df.to_arrow()
    mid = time.perf_counter()
    g.load_edges_from_arrow(df=df_arrow_from_pl, time="block_timestamp", src="inputs_address", dst="outputs_address")
    end = time.perf_counter()
    convert_time = mid - start
    ingestion_time = end - mid
    total_time = end - start
    print(
        f"[polars->arrow] convert {convert_time:.3f}s, ingest {ingestion_time:.3f}s, "
        f"total {total_time:.3f}s;"
    )
    del g, df_arrow_from_pl
    gc.collect()
    return total_time

def bench_polars_to_arrow_streaming(df: pl.DataFrame) -> float:
    g = Graph()
    start = time.perf_counter()
    df_arrow_from_pl = df.to_arrow()
    mid = time.perf_counter()
    g.load_edges_from_arrow_streaming(df=df_arrow_from_pl, time="block_timestamp", src="inputs_address", dst="outputs_address")
    end = time.perf_counter()
    convert_time = mid - start
    ingestion_time = end - mid
    total_time = end - start
    print(
        f"[polars->arrow] with streaming convert {convert_time:.3f}s, ingest {ingestion_time:.3f}s, "
        f"total {total_time:.3f}s;"
    )
    del g, df_arrow_from_pl
    gc.collect()
    return total_time

def bench_duckdb(df: pl.DataFrame) -> float:
    g = Graph()
    df_arrow_from_pl = df.to_arrow()
    start = time.perf_counter()
    duckdb_df = duckdb.sql("SELECT * FROM df_arrow_from_pl")
    mid = time.perf_counter()
    # internally calls fetch_arrow_table() on duckdb_df
    g.load_edges_from_duckdb(df=duckdb_df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    end = time.perf_counter()
    load_time = mid - start
    ingestion_time = end - mid
    total_time = end - start
    print(
        f"[polars->duckdb] load {load_time:.3f}s, ingest {ingestion_time:.3f}s, "
        f"total {total_time:.3f}s;"
    )
    del g, df_arrow_from_pl, duckdb_df
    gc.collect()
    return total_time

def bench_duckdb_streaming(df: pl.DataFrame) -> float:
    g = Graph()
    df_arrow_from_pl = df.to_arrow()
    start = time.perf_counter()
    duckdb_df = duckdb.sql("SELECT * FROM df_arrow_from_pl")
    mid = time.perf_counter()
    # uses the __arrow_c_stream__() interface internally
    g.load_edges_from_duckdb_streaming(df=duckdb_df, time="block_timestamp", src="inputs_address", dst="outputs_address")
    end = time.perf_counter()
    load_time = mid - start
    ingestion_time = end - mid
    total_time = end - start
    print(
        f"[polars->duckdb] streaming load {load_time:.3f}s, ingest {ingestion_time:.3f}s, "
        f"total {total_time:.3f}s;"
    )
    del g, df_arrow_from_pl, duckdb_df
    gc.collect()
    return total_time


def ingestion_speed_btc_dataset():
    df_pd: pd.DataFrame = pd.read_parquet(FLATTENED_FILE)
    df_fireducks: fpd.frame.DataFrame = fpd.read_parquet(FLATTENED_FILE)
    df_pl: pl.DataFrame = pl.read_parquet(FLATTENED_FILE)

    pandas_ingestion_times = []
    fireducks_ingestion_times = []
    pl_native_total_times = []
    pl_to_arrow_total_times = []
    pl_to_arrow_streaming_total_times = []
    duckdb_ingestion_times = []
    duckdb_streaming_ingestion_times = []

    for _ in range(5):
        # 1) Pandas ingestion
        pandas_time = bench_pandas(df_pd)
        pandas_ingestion_times.append(pandas_time)
        gc.collect()

        # 2) Fireducks Pandas ingestion
        fpd_time = bench_fire_ducks_pandas(df_fireducks)
        fireducks_ingestion_times.append(fpd_time)
        gc.collect()

        # 3) to_pandas() called within rust
        polars_native_time = bench_polars_native(df=df_pl)
        pl_native_total_times.append(polars_native_time)
        gc.collect()

        # 4) Arrow ingestion
        arrow_time = bench_polars_to_arrow(df_pl)
        pl_to_arrow_total_times.append(arrow_time)
        gc.collect()

        # 5) Arrow ingestion streaming
        arrow_streaming_time = bench_polars_to_arrow_streaming(df_pl)
        pl_to_arrow_streaming_total_times.append(arrow_streaming_time)
        gc.collect()

        # 6) DuckDB ingestion
        duckdb_time = bench_duckdb(df_pl)
        duckdb_ingestion_times.append(duckdb_time)
        gc.collect()

        # 7) DuckDB streaming ingestion
        duckdb_streaming_time = bench_duckdb_streaming(df_pl)
        duckdb_streaming_ingestion_times.append(duckdb_streaming_time)
        gc.collect()


    formatted_pandas = [f"{num:.3f}s" for num in pandas_ingestion_times]
    formatted_fireducks = [f"{num:.3f}s" for num in fireducks_ingestion_times]
    formatted_pl_native = [f"{num:.3f}s" for num in pl_native_total_times]
    formatted_pl_to_arrow = [f"{num:.3f}s" for num in pl_to_arrow_total_times]
    formatted_pl_to_arrow_streaming = [f"{num:.3f}s" for num in pl_to_arrow_streaming_total_times]
    formatted_duckdb_time = [f"{num:.3f}s" for num in duckdb_ingestion_times]
    formatted_duckdb_streaming_time = [f"{num:.3f}s" for num in duckdb_streaming_ingestion_times]

    print(f"Pandas:\t\t\t\t\t{formatted_pandas}")
    print(f"Fireducks:\t\t\t\t{formatted_fireducks}")
    print(f"Polars native:\t\t\t{formatted_pl_native}")
    print(f"Load from arrow:\t\t{formatted_pl_to_arrow}")
    print(f"Arrow with streaming:\t{formatted_pl_to_arrow_streaming}")
    print(f"Load from duckdb:\t\t{formatted_duckdb_time}")
    print(f"Duckdb with streaming:\t{formatted_duckdb_streaming_time}")


if __name__ == "__main__":
    ingestion_speed_btc_dataset()