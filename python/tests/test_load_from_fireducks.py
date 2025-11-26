try:
    import fireducks.pandas as fpd
except ModuleNotFoundError:
    fpd = None

if fpd:
    import pandas
    from raphtory import Graph
    def _collect_edges(g: Graph):
        return sorted((e.history()[0], e.src.id, e.dst.id, e["value"]) for e in g.edges)

    def test_load_edges_from_fireducks_df():
        # FireDucks DataFrame (pandas-compatible API)
        df = fpd.DataFrame(
            {
                "time": [1, 2, 3],
                "src": [1, 2, 3],
                "dst": [2, 3, 4],
                "value": [10.0, 20.0, 30.0],
            }
        )

        g: Graph = Graph()
        g.load_edges_from_pandas(df=df, time="time", src="src", dst="dst", properties=["value"])
        assert [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)] == _collect_edges(g)

    def test_fireducks_matches_pandas_for_same_edges():
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

        g_fireducks: Graph = Graph()
        g_fireducks.load_edges_from_pandas(df=df_fireducks, time="time", src="src", dst="dst", properties=["value"])

        g_pandas = Graph()
        g_pandas.load_edges_from_pandas(df=df_pandas, time="time", src="src", dst="dst", properties=["value"])

        expected = [(1, 1, 2, 10.0), (2, 2, 3, 20.0), (3, 3, 4, 30.0)]

        assert _collect_edges(g_fireducks) == _collect_edges(g_pandas)
        assert _collect_edges(g_fireducks) == expected
        assert _collect_edges(g_pandas) == expected