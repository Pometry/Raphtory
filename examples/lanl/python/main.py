from raphtory import ArrowGraph
from raphtory.lanl import lanl_query1, lanl_query2, lanl_query3, lanl_query3b, lanl_query3c, lanl_query4, exfilteration_query1, exfilteration_count_query_total, exfiltration_list_query_count
from raphtory import algorithms
import time
from typing import TypeVar, Callable
import os
import sys


B = TypeVar('B')

def measure(name: str, f: Callable[..., B], *args, print_result: bool = True) -> B:
    start_time = time.time()
    result = f(*args)
    elapsed_time = time.time() - start_time
    
    time_unit = "s"
    elapsed_time_display = elapsed_time
    if elapsed_time < 1:
        time_unit = "ms"
        elapsed_time_display *= 1000

    if print_result:
        print(f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}, result: {result}")
    else:
        print(f"Running {name}: time: {elapsed_time_display:.3f}{time_unit}")

    return result


def main():
    # Retrieve mandatory parameters from environment variables
    resources_dir = os.getenv('resources_dir')
    graph_dir = os.getenv('target_dir')

    if resources_dir is None or graph_dir is None:
        raise ValueError("Both 'resources_dir' and 'target_dir' environment variables must be set.")

    # Retrieve optional parameters from environment variables or set default values
    chunk_size = int(os.getenv('chunk_size', '268435456'))
    t_props_chunk_size = int(os.getenv('t_props_chunk_size', '20000000'))
    read_chunk_size = int(os.getenv('read_chunk_size', '4000000'))
    concurrent_files = int(os.getenv('concurrent_files', '1'))
    num_threads = int(os.getenv('num_threads', '4'))
    
    print(f"Resources directory: {resources_dir}")
    print(f"Target directory: {graph_dir}")
    print(f"Chunk size: {chunk_size}")
    print(f"t_props chunk size: {t_props_chunk_size}")
    print(f"Read chunk size: {read_chunk_size}")
    print(f"Concurrent files: {concurrent_files}")
    print(f"Number of threads: {num_threads}")

    layer_parquet_cols = [
        {
            "parquet_dir": os.path.join(resources_dir, "netflowsorted/nft_sorted"),
            "layer": "netflow",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": os.path.join(resources_dir, "netflowsorted/v1_sorted"),
            "layer": "events_1v",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": os.path.join(resources_dir, "netflowsorted/v2_sorted"),
            "layer": "events_2v",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        }
    ]

    # # Read the Parquet file
    # table = pq.read_table(parquet_dir + '/part-00000-8b31eaa4-2bd9-4f07-b61c-a353aed2af22-c000.snappy.parquet')
    # print(table.schema)

    print()
    try:
        g = measure("Graph load from dir", ArrowGraph.load_from_dir, graph_dir, print_result=False)
    except Exception as e:
        g = measure(
            "Graph load from parquets", 
            ArrowGraph.load_from_parquets,
            graph_dir,
            layer_parquet_cols,
            chunk_size, 
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            print_result=False
        )

    print("Nodes count =", g.count_nodes())
    print("Edges count =", g.count_edges())
    print("Earliest time =", g.earliest_time)
    print("Latest time =", g.latest_time)

    measure("Query 1", lanl_query1, g)
    measure("Query 2", lanl_query2, g)
    measure("Query 3", lanl_query3, g)
    measure("Query 3b", lanl_query3b, g)
    # assert(measure("Query 3c", lanl_query3c, g) == 0)
    measure("Query 4", lanl_query4, g)

    measure("CC", algorithms.connected_components, g, print_result=False)
    measure("Weakly CC  Layer", algorithms.weakly_connected_components, g.layer("netflow"), 20, print_result=False)
    measure("Weakly CC", algorithms.weakly_connected_components, g, 20, print_result=False)
    measure("Page Rank", algorithms.pagerank, g, 100, print_result=False)
    
    measure("Exfilteration Query 1", exfilteration_query1, g)
    measure("Exfilteration Count Query Total", exfilteration_count_query_total, g, 30)
    measure("Exfilteration List Query Count", exfiltration_list_query_count, g, 30)

if __name__ == "__main__":
    main()
