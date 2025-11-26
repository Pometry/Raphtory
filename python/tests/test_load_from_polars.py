import polars as pl
from raphtory import Graph
import pytest

def _collect_edges(g: Graph):
    return sorted((e.history()[0], e.src.id, e.dst.id, e["value"]) for e in g.edges)

def test_load_edges_from_polars_df_error():
    df = pl.DataFrame(
        {
            "time": [1, 2, 3],
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
            "value": [10.0, 20.0, 30.0],
        }
    )

    g = Graph()
    with pytest.raises(Exception) as e:
        # Current loader expects a pandas DataFrame; this will fail in pyarrow.Table.from_pandas
        g.load_edges_from_pandas(df=df, time="time", src="src", dst="dst", properties=["value"])

    print(f"\nCaptured error: {str(e.value)}")

def test_load_edges_from_polars_df_via_to_pandas():
    df = pl.DataFrame(
        {
            "time": [1, 2, 3],
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
            "value": [10.0, 20.0, 30.0],
        }
    )

    g = Graph()
    g.load_edges_from_pandas(df=df.to_pandas(), time="time", src="src", dst="dst", properties=["value"])
    expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]
    assert _collect_edges(g) == expected

def test_load_edges_from_polars_df():
    df = pl.DataFrame(
        {
            "time": [1, 2, 3],
            "src": [1, 2, 3],
            "dst": [2, 3, 4],
            "value": [10.0, 20.0, 30.0],
        }
    )

    g = Graph()
    g.load_edges_from_df(data=df, time="time", src="src", dst="dst", properties=["value"])
    expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]
    assert _collect_edges(g) == expected