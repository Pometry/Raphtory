from raphtory import Graph
from custom_python_extension import custom_algorithm
from pytest import raises


def test_custom_algorithm():
    g = Graph()
    for v in range(10):
        g.add_vertex(0, v)
    assert custom_algorithm(g) == 10


def test_error_for_wrong_type():
    """ calling with the wrong type should still raise a type error (unless it defines a bincode method)"""
    with raises(TypeError):
        custom_algorithm(1)
