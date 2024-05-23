import argparse
from raphtory import ArrowGraph
from raphtory import algorithms
import time
from typing import TypeVar, Callable
from tqdm import tqdm
import os
import sys
from raphtory import ArrowGraph, Query, State


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

def hop_queries(graph, nodes, layer, hops, temporal, start=None, keep_path=False):
    q = Query.from_node_ids(nodes)
    
    for _ in range(hops):
        q = q.out(layer)
    
    if temporal:
        max_duration = 2**31 - 1
        state = State.path_window(keep_path=keep_path, start_t=start, duration=max_duration)
    else:
        state = State.no_state()
    
    q.run(graph, state)

def static_multi_hop_single_node_query(graph: ArrowGraph):
    hop_queries(graph, [0], "default", 3, False)

def static_multi_hop_multi_node_query(graph: ArrowGraph):
    nodes = list(range(100))
    hop_queries(graph, nodes, "default", 3, False)
    
def multi_hop_single_node_query(graph: ArrowGraph):
    hop_queries(graph, [0], "default", 3, True, 10, True)

def multi_hop_multi_node_query(graph: ArrowGraph):
    nodes = list(range(100))
    hop_queries(graph, nodes, "default", 3, True, 10, True)

def main(graph_dir, resources_dir, chunk_size, t_props_chunk_size, read_chunk_size, concurrent_files, num_threads):

    if resources_dir is None or graph_dir is None:
        raise ValueError("Both 'resources_dir' and 'target_dir' environment variables must be set.")

    print(f"Resources directory: {resources_dir}")
    print(f"Target directory: {graph_dir}")
    print(f"Chunk size: {chunk_size}")
    print(f"t_props chunk size: {t_props_chunk_size}")
    print(f"Read chunk size: {read_chunk_size}")
    print(f"Concurrent files: {concurrent_files}")
    print(f"Number of threads: {num_threads}")

    layer_parquet_cols = [
        {
            "parquet_dir": resources_dir,
            "layer": "default",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "time",
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
            None,
            chunk_size, 
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
            print_result=False
        )

    print("Nodes count =", g.count_nodes())
    print("Edges count =", g.count_edges())
    print("Earliest time =", g.earliest_time)
    print("Latest time =", g.latest_time)
    
    measure("Static Multi Hop Single Node Query", static_multi_hop_single_node_query, g, print_result=False)
    measure("Static Multi Hop Multi Node Query", static_multi_hop_multi_node_query, g, print_result=False)
    measure("Temporal Multi Hop Singl Node Query", multi_hop_single_node_query, g, print_result=False)
    measure("Temporal Multi Hop Multi Node Query", multi_hop_multi_node_query, g, print_result=False)
    measure("CC", algorithms.connected_components, g, print_result=False)
    measure("Page Rank", algorithms.pagerank, g, 100, print_result=False)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run LANL example queries')
    parser.add_argument('--graph-dir', type=str, help='The directory of the graph')
    parser.add_argument('--resources-dir', type=str, help='paths to the parquet directory')
    parser.add_argument('--chunk-size', type=int, default=268435456, help='Chunk size')
    parser.add_argument('--t-props-chunk-size', type=int, default=20000000, help='t_props chunk size')
    parser.add_argument('--read-chunk-size', type=int, default=4000000, help='Read chunk size')
    parser.add_argument('--concurrent-files', type=int, default=1, help='Concurrent files')
    parser.add_argument('--num-threads', type=int, default=4, help='Number of threads')
    args = parser.parse_args()
    main(args.graph_dir, args.resources_dir, args.chunk_size, args.t_props_chunk_size, args.read_chunk_size, args.concurrent_files, args.num_threads)
