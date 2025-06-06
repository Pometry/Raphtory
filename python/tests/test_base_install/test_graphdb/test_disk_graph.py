from raphtory import algorithms
import pandas as pd
import tempfile
from utils import measure
import os

edges = pd.DataFrame(
    {
        "src": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        "dst": [2, 3, 4, 5, 1, 3, 4, 5, 1, 2, 4, 5, 1, 2, 3, 5, 1, 2, 3, 4],
        "time": [
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
            110,
            120,
            130,
            140,
            150,
            160,
            170,
            180,
            190,
            200,
        ],
    }
).sort_values(["src", "dst", "time"])


# in every test use with to create a temporary directory that will be deleted automatically
# after the with block ends

if "DISK_TEST_MARK" in os.environ:

    def test_counts():
        from raphtory import DiskGraphStorage

        graph_dir = tempfile.TemporaryDirectory()
        graph = DiskGraphStorage.load_from_pandas(
            graph_dir.name, edges, "time", "src", "dst"
        )
        graph = graph.to_events()
        assert graph.count_nodes() == 5
        assert graph.count_edges() == 20

    def test_disk_graph():
        from raphtory import DiskGraphStorage

        curr_dir = os.path.dirname(os.path.abspath(__file__))
        rsc_dir = os.path.join(
            curr_dir, "..", "..", "..", "..", "pometry-storage-private", "resources"
        )
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
                DiskGraphStorage.load_from_dir,
                graph_dir,
                print_result=False,
            )
        except Exception as e:
            chunk_size = 268_435_456
            num_threads = 4
            t_props_chunk_size = int(chunk_size / 8)

            g = measure(
                "Graph load from parquets",
                DiskGraphStorage.load_from_parquets,
                graph_dir.name,
                layer_parquet_cols,
                None,
                chunk_size,
                t_props_chunk_size,
                num_threads,
                None,
                print_result=False,
            )

        g = g.to_events()

        assert g.count_nodes() == 1624
        assert g.layer("netflow").count_edges() == 2018
        assert g.layer("netflow").count_nodes() == 1619
        assert g.earliest_time == 7257601
        assert g.latest_time == 7343985

        actual = measure(
            "Weakly CC  Layer",
            algorithms.weakly_connected_components,
            g.layer("netflow"),
            20,
            print_result=False,
        )
        assert len(list(actual)) == 1619

        actual = measure(
            "Weakly CC",
            algorithms.weakly_connected_components,
            g,
            20,
            print_result=False,
        )
        assert len(list(actual)) == 1624

        actual = measure(
            "Page Rank",
            algorithms.pagerank,
            g.layer("netflow"),
            100,
            print_result=False,
        )
        assert len(list(actual)) == 1619

    def test_disk_graph_type_filter():
        from raphtory import DiskGraphStorage

        curr_dir = os.path.dirname(os.path.abspath(__file__))
        rsc_dir = os.path.join(
            curr_dir, "..", "..", "..", "..", "pometry-storage-private", "resources"
        )
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
            }
        ]

        chunk_size = 268_435_456
        num_threads = 4
        t_props_chunk_size = int(chunk_size / 8)

        g = DiskGraphStorage.load_from_parquets(
            graph_dir.name,
            layer_parquet_cols,
            rsc_dir + "/netflowsorted/props/props.parquet",
            chunk_size,
            t_props_chunk_size,
            num_threads,
            "node_type",
        ).to_events()

        assert g.count_nodes() == 1619
        assert g.layer("netflow").count_edges() == 2018
        assert g.earliest_time == 7257619
        assert g.latest_time == 7343970

        assert len(g.nodes.type_filter(["A"]).name.collect()) == 785
        assert len(g.nodes.type_filter([""]).name.collect()) == 0
        assert len(g.nodes.type_filter(["A", "B"]).name.collect()) == 1619

        neighbor_names = g.nodes.type_filter(["A"]).neighbours.name.collect()
        total_length = sum(len(names) for names in neighbor_names)
        assert total_length == 2056

        assert g.nodes.type_filter([]).name.collect() == []

        neighbor_names = (
            g.nodes.type_filter(["A"]).neighbours.type_filter(["B"]).name.collect()
        )
        total_length = sum(len(names) for names in neighbor_names)
        assert total_length == 1023

        assert g.node("Comp175846").neighbours.type_filter(["A"]).name.collect() == [
            "Comp844043"
        ]
        assert g.node("Comp175846").neighbours.type_filter(["B"]).name.collect() == []
        assert g.node("Comp175846").neighbours.type_filter([]).name.collect() == []
        assert g.node("Comp175846").neighbours.type_filter(
            ["A", "B"]
        ).name.collect() == ["Comp844043"]

        neighbor_names = g.node("Comp175846").neighbours.neighbours.name.collect()
        assert len(neighbor_names) == 193
