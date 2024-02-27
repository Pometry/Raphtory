from raphtory import ArrowGraph
import pyarrow.parquet as pq


def test_arrow_graph():
    graph_dir = "target"
    parquet_dir = "data/netflowsorted/nft_sorted"
    src_col = "src"
    src_hash_col = "src_hash"
    dst_col = "dst"
    dst_hash_col = "dst_hash"
    time_col = "epoch_time"

    # # Read the Parquet file
    # table = pq.read_table(parquet_dir + '/part-00000-8b31eaa4-2bd9-4f07-b61c-a353aed2af22-c000.snappy.parquet')
    # print(table.schema)

    try:
        g = ArrowGraph.open_path(graph_dir)
    except Exception as e:
        g = ArrowGraph.from_parquets(
            graph_dir,
            parquet_dir,
            src_col,
            src_hash_col,
            dst_col,
            dst_hash_col,
            time_col
        )

    print("Node count", g.count_nodes())
    print("Edge count", g.count_edges())
    print("Earliest time", g.earliest_time)
    print("Latest time", g.latest_time)
