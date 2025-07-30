from __future__ import unicode_literals
import random
from raphtory import Graph
import pytest

def test_metadata_props():
    g = Graph()
    n = g.add_node(1,1,properties={"prop1":"p1"})
    n.add_metadata({"meta1":"m1"})
    n = g.add_node(1,2,properties={"prop2":"p2"})
    n.add_metadata({"meta2":"m2"})
    e = g.add_edge(2, 1, 2, properties={"prop3":"p3"})
    e.add_metadata({"meta3":"m3"})
    e = g.add_edge(2, 2, 1, properties={"prop4":"p4"})
    e.add_metadata({"meta4":"m4"})

    assert g.nodes.properties.keys() == ['prop1', 'prop2']
    assert g.node(1).properties.keys() == ['prop1']
    assert g.node(2).properties.keys() == ['prop2']

    assert g.nodes.metadata.keys() == ['meta1', 'meta2']
    assert g.node(1).metadata.keys() == ['meta1']
    assert g.node(2).metadata.keys() == ['meta2']

    assert g.window(1,2).nodes.properties.keys() == ['prop1', 'prop2']
    assert g.window(1,2).nodes.metadata.keys() == ['meta1', 'meta2']

    assert g.edges.properties.keys() == ['prop3', 'prop4']
    assert g.edge(1, 2).properties.keys() == ['prop3']
    assert g.edge(2, 1).properties.keys() == ['prop4']

    assert g.edges.metadata.keys() == ['meta3', 'meta4']
    assert g.edge(1, 2).metadata.keys() == ['meta3']
    assert g.edge(2, 1).metadata.keys() == ['meta4']

    assert g.window(2,3).edges.properties.keys() == ['prop3', 'prop4']
    assert g.window(2,3).edges.metadata.keys() == ['meta3', 'meta4']


## Benchmark
benchmark_means = {}
sizes = [100_000, 500_000]
size_ids = ["small", "large"]


def create_graph(n):
    g = Graph()

    props = [
        {"name": "Laptop", "brand": "Dell", "ram": "16GB"},
        {"title": "Inception", "director": "Christopher Nolan", "year": 2010},
        {"city": "Paris", "country": "France", "population_millions": 2.1},
        {"product": "Chair", "material": "Wood"},
        {"animal": "Tiger", "habitat": "Forest", "endangered": True}
    ]

    for i in range(n):
        shared_props = random.choice(props)
        shared_meta = random.choice(props)

        node = g.add_node(1, i, properties=shared_props)
        node.add_metadata(shared_meta)

        if i > 0:
            j = random.randint(0, i - 1)
            edge = g.add_edge(2, i, j, properties=shared_props)
            edge.add_metadata(shared_meta)

    return g


@pytest.fixture(scope="function")
def graph(request):
    size = request.param
    return create_graph(size)


def record_benchmark_mean(benchmark, request, group_name):
    stats = benchmark.stats.stats
    benchmark_means[f"{group_name}[{request.node.callspec.id}]"] = stats.mean


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="nodes_global_properties_keys")
def test_nodes_properties_keys_global(benchmark, graph, request):
    benchmark(lambda: list(graph.nodes.properties.keys()))
    record_benchmark_mean(benchmark, request, "nodes_global_properties_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="nodes_global_metadata_keys")
def test_nodes_metadata_keys_global(benchmark, graph, request):
    benchmark(lambda: graph.nodes.metadata.keys())
    record_benchmark_mean(benchmark, request, "nodes_global_metadata_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="nodes_windowed_properties_keys")
def test_nodes_properties_keys_windowed(benchmark, graph, request):
    benchmark(lambda: graph.window(1, 2).nodes.properties.keys())
    record_benchmark_mean(benchmark, request, "nodes_windowed_properties_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="nodes_windowed_metadata_keys")
def test_nodes_metadata_keys_windowed(benchmark, graph, request):
    benchmark(lambda: graph.window(1, 2).nodes.metadata.keys())
    record_benchmark_mean(benchmark, request, "nodes_windowed_metadata_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="edges_global_properties_keys")
def test_edges_properties_keys_global(benchmark, graph, request):
    benchmark(lambda: graph.edges.properties.keys())
    record_benchmark_mean(benchmark, request, "edges_global_properties_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="edges_global_metadata_keys")
def test_edges_metadata_keys_global(benchmark, graph, request):
    benchmark(lambda: graph.edges.metadata.keys())
    record_benchmark_mean(benchmark, request, "edges_global_metadata_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="edges_windowed_properties_keys")
def test_edges_properties_keys_windowed(benchmark, graph, request):
    benchmark(lambda: graph.window(1, 2).edges.properties.keys())
    record_benchmark_mean(benchmark, request, "edges_windowed_properties_keys")


@pytest.mark.parametrize("graph", sizes, indirect=True, ids=size_ids)
@pytest.mark.benchmark(group="edges_windowed_metadata_keys")
def test_edges_metadata_keys_windowed(benchmark, graph, request):
    benchmark(lambda: graph.window(1, 2).edges.metadata.keys())
    record_benchmark_mean(benchmark, request, "edges_windowed_metadata_keys")


# Regression Check
def test_benchmark_regression_limit():
    max_pct_diff = 100.0
    grouped = {}
    results = []
    has_failures = False
    print()

    for name, mean in benchmark_means.items():
        group = name.split("[")[0]
        size = name.split("[")[1].rstrip("]")
        grouped.setdefault(group, {})[size] = mean

    for group, sizes in grouped.items():
        if "small" in sizes and "large" in sizes:
            mean_small = sizes["small"]
            mean_large = sizes["large"]

            # Avoid division by zero
            if mean_small == 0 or mean_large == 0:
                pct_diff = float('inf')
            else:
                pct_diff = abs(mean_large - mean_small) / mean_small * 100

            if pct_diff > max_pct_diff:
                has_failures = True
                results.append(
                    f"{group}: ❌ {pct_diff:.2f}% divergence "
                    f"(small = {mean_small:.6f}, large = {mean_large:.6f}, "
                    f"allowed = {max_pct_diff:.2f}%)"
                )
            else:
                results.append(
                    f"{group}: ✅ OK - {pct_diff:.2f}% divergence "
                    f"(small = {mean_small:.6f}, large = {mean_large:.6f})"
                )

    for line in results:
        print(line)

    if has_failures:
        pytest.fail("Benchmark regression limit exceeded.")