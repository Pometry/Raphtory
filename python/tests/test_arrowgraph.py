from raphtory import ArrowGraph
from raphtory.lanl import (
    lanl_query1,
    lanl_query2,
    lanl_query3,
    lanl_query3b,
    lanl_query3c,
    lanl_query4,
    exfilteration_query1,
    exfilteration_count_query_total,
    exfiltration_list_query_count,
)
from raphtory import algorithms
from utils import measure
import tempfile
import os


def test_arrow_graph():
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    rsc_dir = os.path.join(curr_dir, "..", "..", "resource")
    rsc_dir = os.path.normpath(rsc_dir)
    print("rsc_dir:", rsc_dir + "/netflowsorted/nft_sorted")

    graph_dir = tempfile.TemporaryDirectory()
    layer_parquet_cols = [
        {
            "parquet_dir": rsc_dir + "/netflowsorted/nft_sorted",
            "layer": "netflow",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": rsc_dir + "/netflowsorted/v1_sorted",
            "layer": "events_1v",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        },
        {
            "parquet_dir": rsc_dir + "/netflowsorted/v2_sorted",
            "layer": "events_2v",
            "src_col": "src",
            "dst_col": "dst",
            "time_col": "epoch_time",
        },
    ]

    # # Read the Parquet file
    # table = pq.read_table(parquet_dir + '/part-00000-8b31eaa4-2bd9-4f07-b61c-a353aed2af22-c000.snappy.parquet')
    # print(table.schema)

    print()
    try:
        g = measure(
            "Graph load from dir",
            ArrowGraph.load_from_dir,
            graph_dir,
            print_result=False,
        )
    except Exception as e:
        chunk_size = 268_435_456
        num_threads = 4
        t_props_chunk_size = int(chunk_size / 8)
        read_chunk_size = 4_000_000
        concurrent_files = 1

        g = measure(
            "Graph load from parquets",
            ArrowGraph.load_from_parquets,
            graph_dir.name,
            layer_parquet_cols,
            None,
            chunk_size,
            t_props_chunk_size,
            read_chunk_size,
            concurrent_files,
            num_threads,
            print_result=False,
        )

    assert g.count_nodes() == 1624
    assert g.layer("netflow").count_edges() == 2018
    assert g.earliest_time == 7257601
    assert g.latest_time == 7343985

    assert measure("Query 1", lanl_query1, g) == 0
    assert measure("Query 2", lanl_query2, g) == 0
    assert measure("Query 3", lanl_query3, g) == 0
    assert measure("Query 3b", lanl_query3b, g) == 0
    # assert(measure("Query 3c", lanl_query3c, g) == 0)
    assert measure("Query 4", lanl_query4, g) == 0

    # assert (measure("CC", algorithms.connected_components, g, print_result=False)[:10] == [0, 1, 2, 3, 4, 5, 6, 7, 8,
    #                                                                                        9])

    actual = measure(
        "Weakly CC  Layer",
        algorithms.weakly_connected_components,
        g.layer("netflow"),
        20,
        print_result=False,
    )
    assert len(list(actual.get_all_with_names())) == 1624

    # Doesn't work yet (was silently running on only the first layer before but now actually panics because of lack of multilayer edge views)
    # actual = measure("Weakly CC", algorithms.weakly_connected_components, g, 20, print_result=False)
    # assert len(list(actual.get_all_with_names())) == 1624

    actual = measure(
        "Page Rank", algorithms.pagerank, g.layer("netflow"), 100, print_result=False
    )
    assert len(list(actual.get_all_with_names())) == 1624

    assert measure("Exfilteration Query 1", exfilteration_query1, g) == 0
    assert (
        measure(
            "Exfilteration Count Query Total", exfilteration_count_query_total, g, 30
        )
        == 0
    )
    assert (
        measure("Exfilteration List Query Count", exfiltration_list_query_count, g, 30)
        == 0
    )
