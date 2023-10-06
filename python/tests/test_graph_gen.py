def test_karate_club():
    from raphtory.graph_loader import karate_club_graph
    g = karate_club_graph()
    print(g.vertices())
    assert g.count_vertices() == 34
    assert g.count_edges() == 78
