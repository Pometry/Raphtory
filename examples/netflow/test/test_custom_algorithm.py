from raphtory_netflow import Graph
from raphtory_netflow.algorithms import netflow_one_path_node
from pytest import raises


def test_one_path():
    graph = Graph()
    graph.add_edge(0, 1, 2, layer="Events2v4624")
    graph.add_edge(1, 2, 2, layer="Events1v4688")
    graph.add_edge(2, 2, 3, {"dstBytes": 100_000_005}, "Netflow")

    actual = netflow_one_path_node(graph, True)
    assert actual == 1


def test_error_for_wrong_type():
    """calling with the wrong type should still raise a type error (unless it defines a bincode method)"""
    with raises(TypeError):
        netflow_one_path_node(1, True)
