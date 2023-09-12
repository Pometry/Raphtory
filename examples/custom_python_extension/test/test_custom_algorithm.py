from raphtory import Graph
from custom_python_extension import custom_algorithm


def test_custom_algorithm():
    g = Graph()
    for v in range(10):
        g.add_vertex(0, v)
    assert custom_algorithm(g) == 10
