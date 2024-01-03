def test_karate_club():
    from raphtory.graph_loader import karate_club_graph
    g = karate_club_graph()
    assert g.count_nodes() == 34
    assert g.count_edges() == 155

