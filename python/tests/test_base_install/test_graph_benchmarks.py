from __future__ import unicode_literals
from raphtory import Graph, PersistentGraph
import pytest

def test_metadata_props():
    g = Graph()
    n = g.add_node(1,1,properties={"prop1":"p1"})
    n.add_metadata({"meta1":"m1"})
    n = g.add_node(1,2,properties={"prop2":"p2"})
    n.add_metadata({"meta2":"m2"})

    assert g.nodes.properties.keys() == ['prop1', 'prop2']
    assert g.node(1).properties.keys() == ['prop1']
    assert g.node(2).properties.keys() == ['prop2']

    assert g.nodes.metadata.keys() == ['meta1', 'meta2']
    assert g.node(1).metadata.keys() == ['meta1']
    assert g.node(2).metadata.keys() == ['meta2']


    assert g.window(1,2).nodes.properties.keys() == ['prop1', 'prop2']
    assert g.window(1,2).nodes.metadata.keys() == ['meta1', 'meta2']


def create_graph(n=100_000):
    g = Graph()
    for i in range(n):
        node = g.add_node(1, i, properties={f"prop{i}": f"p{i}"})
        node.add_metadata({f"meta{i}": f"m{i}"})
    return g


@pytest.fixture(scope="module")
def large_graph():
    return create_graph(n=2)


@pytest.mark.benchmark(group="global_properties_keys")
def test_properties_keys_global(benchmark, large_graph):
    benchmark(lambda: large_graph.nodes.properties.keys())


@pytest.mark.benchmark(group="global_metadata_keys")
def test_metadata_keys_global(benchmark, large_graph):
    benchmark(lambda: large_graph.nodes.metadata.keys())


@pytest.mark.benchmark(group="windowed_properties_keys")
def test_properties_keys_windowed(benchmark, large_graph):
    benchmark(lambda: large_graph.window(1, 2).nodes.properties.keys())


@pytest.mark.benchmark(group="windowed_metadata_keys")
def test_metadata_keys_windowed(benchmark, large_graph):
    benchmark(lambda: large_graph.window(1, 2).nodes.metadata.keys())
