from raphtory import ArrowGraph
from raphtory.lanl import lanl_query1, lanl_query2, lanl_query3, lanl_query3b, lanl_query3c, lanl_query4
from raphtory import algorithms
from utils import measure


def test_arrow_graph():
    graph_dir = "target"
    layer_parquet_cols = [
        {
            "parquet_dir": "data/netflowsorted/nft_sorted",
            "layer": "netflow",
            "src_col": "src",
            "src_hash_col": "src_hash",
            "dst_col": "dst",
            "dst_hash_col": "dst_hash",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": "data/netflowsorted/v1_sorted",
            "layer": "events_1v",
            "src_col": "src",
            "src_hash_col": "src_hash",
            "dst_col": "dst",
            "dst_hash_col": "dst_hash",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": "data/netflowsorted/v2_sorted",
            "layer": "events_2v",
            "src_col": "src",
            "src_hash_col": "src_hash",
            "dst_col": "dst",
            "dst_hash_col": "dst_hash",
            "time_col": "epoch_time",
        }
    ]

    # # Read the Parquet file
    # table = pq.read_table(parquet_dir + '/part-00000-8b31eaa4-2bd9-4f07-b61c-a353aed2af22-c000.snappy.parquet')
    # print(table.schema)

    print()
    print("Loading the graph")
    try:
        print("Attempting to load the graph from the directory")
        g = ArrowGraph.load_from_dir(graph_dir)
    except Exception as e:
        print("Failed to load the graph from the directory. Attempting to load from parquet files ", e)
        
        chunk_size = 268_435_456
        num_threads = 4
        t_props_chunk_size = int(chunk_size / 8)
        read_chunk_size = 4_000_000
        concurrent_files = 1
        
        g = ArrowGraph.load_from_parquets(
            graph_dir,
            layer_parquet_cols,
            chunk_size, 
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
        )

    print("Node count", g.count_nodes())
    print("Edge count", g.count_edges())
    print("Earliest time", g.earliest_time)
    print("Latest time", g.latest_time)

    measure("Query 1", lanl_query1, g)
    measure("Query 2", lanl_query2, g)
    measure("Query 3", lanl_query3, g)
    measure("Query 3b", lanl_query3b, g)
    # measure("Query 3c", lanl_query3c, g)
    measure("Query 4", lanl_query4, g)

    measure("CC", algorithms.connected_components, g, print_result = False)
    measure("Weakly CC  Layer", algorithms.weakly_connected_components, g.layer("netflow"), 20, print_result = False)
    measure("Weakly CC", algorithms.weakly_connected_components, g, 20, print_result = False)
    measure("Page Rank", algorithms.pagerank, g, 100, print_result = False)
