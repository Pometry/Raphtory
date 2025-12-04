from pathlib import Path

import polars as pl
import pandas as pd
from raphtory import Graph, PersistentGraph
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
    g = Graph()
    file_path_str = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/subset/flattened_data_subset.parquet"
    num_nodes_ingested = []

    # test path string for file
    g.load_nodes(data=file_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g

    # test Path object for file
    file_path_obj = Path(file_path_str)
    g = Graph()
    g.load_nodes(data=file_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, file_path_obj

    # test path string for directory
    dir_path_str = "/Users/arien/RustroverProjects/Raphtory/dataset_tests/subset"
    g = Graph()
    g.load_nodes(data=dir_path_str, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, dir_path_str

    # test Path object for directory
    dir_path_obj = Path("/Users/arien/RustroverProjects/Raphtory/dataset_tests/subset")
    g = Graph()
    g.load_nodes(data=dir_path_obj, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, dir_path_obj

    # test pandas
    df_pd = pd.read_parquet(file_path_str)
    g = Graph()
    g.load_nodes(data=df_pd, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, df_pd

    # test polars
    df_pl = pl.read_parquet(file_path_str)
    g = Graph()
    g.load_nodes(data=df_pl, time="block_timestamp", id="inputs_address")
    num_nodes_ingested.append(len(g.nodes))
    del g, df_pl

    # sanity check, make sure we ingested the same number of nodes each time
    print(f"Number of tests ran: {len(num_nodes_ingested)}")
    for i in range(len(num_nodes_ingested)-1):
        assert num_nodes_ingested[0] == num_nodes_ingested[i+1]


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