def test_karate_club():
    from raphtory.graph_loader import karate_club_graph

    g = karate_club_graph()
    assert g.count_nodes() == 34
    assert g.count_edges() == 155


def _write_reddit_file(path):
    header = "SOURCE_SUBREDDIT\tTARGET_SUBREDDIT\tPOST_ID\tTIMESTAMP\tLINK_SENTIMENT\tPROPERTIES\n"
    properties = ",".join(["1.0"] * 21)
    path.write_text(header + f"a\tb\tp1\t2014-01-01 02:59:18\t1\t{properties}\n")


def test_reddit_hyperlink_graph_local_stores_milliseconds(tmp_path):
    from raphtory.graph_loader import reddit_hyperlink_graph_local

    path = tmp_path / "reddit.tsv"
    _write_reddit_file(path)

    g = reddit_hyperlink_graph_local(str(path))
    assert g.edge("a", "b").earliest_time.t == 1388545158000
    assert g.window("2014-01-01", "2014-01-02").count_edges() == 1
