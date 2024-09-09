from raphtory import PersistentGraph


def test_edge_deletions():
    g = PersistentGraph()
    e = g.add_edge(1, 2, 3)
    e.delete(5)
    e.add_updates(10)
    assert e.history() == [1, 10]
    assert e.deletions() == [5]
