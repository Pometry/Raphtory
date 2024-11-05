from raphtory import PersistentGraph
from numpy.testing import assert_equal as check_arr


def test_edge_deletions():
    g = PersistentGraph()
    e = g.add_edge(1, 2, 3)
    e.delete(5)
    e.add_updates(10)
    check_arr(e.history(), [1, 10])
    check_arr(e.deletions(), [5])
