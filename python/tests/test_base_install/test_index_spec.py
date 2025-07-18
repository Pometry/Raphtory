from raphtory import Graph, IndexSpecBuilder
from raphtory import filter


def init_graph(graph):
    nodes = [
        (1, "pometry", {"p1": 5, "p2": 50}, "fire_nation", {"x": True}),
        (1, "raphtory", {"p1": 10, "p2": 100}, "water_tribe", {"y": False}),
    ]
    for t, name, props, group, const_props in nodes:
        n = graph.add_node(t, name, props, group)
        n.add_constant_properties(const_props)

    edges = [
        (1, "pometry", "raphtory", {"e_p1": 3.2, "e_p2": 10.0}, {"e_x": True}),
        (1, "raphtory", "pometry", {"e_p1": 4.0, "e_p2": 20.0}, {"e_y": False}),
    ]
    for t, src, dst, props, const_props in edges:
        e = graph.add_edge(t, src, dst, props)
        e.add_constant_properties(const_props)

    return graph


def search_nodes(graph, filter_expr):
    return sorted(n.name for n in graph.search_nodes(filter_expr, 10, 0))


def search_edges(graph, filter_expr):
    return sorted(
        f"{e.src.name}->{e.dst.name}" for e in graph.search_edges(filter_expr, 10, 0)
    )


def test_with_all_props_index_spec():
    graph = init_graph(Graph())
    spec = IndexSpecBuilder(graph).with_all_node_props().with_all_edge_props().build()

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("p1") == 5
    f2 = filter.Node.property("x") == True
    assert search_nodes(graph, f1 & f2) == ["pometry"]

    f1 = filter.Edge.property("e_p1") < 5.0
    f2 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1 & f2)) == sorted(["raphtory->pometry"])


def test_with_selected_props_index_spec():
    graph = init_graph(Graph())
    spec = (
        IndexSpecBuilder(graph)
        .with_const_node_props(["y"])
        .with_temp_node_props(["p1"])
        .with_const_edge_props(["e_y"])
        .with_temp_edge_props(["e_p1"])
        .build()
    )

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("p1") == 5
    f2 = filter.Node.property("y") == False
    assert sorted(search_nodes(graph, f1 | f2)) == sorted(["pometry", "raphtory"])

    f = filter.Node.property("y") == False
    assert search_nodes(graph, f) == ["raphtory"]

    f1 = filter.Edge.property("e_p1") < 5.0
    f2 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1 | f2)) == sorted(
        ["pometry->raphtory", "raphtory->pometry"]
    )


def test_with_invalid_property_returns_error():
    graph = init_graph(Graph())
    try:
        IndexSpecBuilder(graph).with_const_node_props(["xyz"])
        assert False, "Expected error for unknown property"
    except Exception as e:
        assert "xyz" in str(e)


def test_build_empty_spec_by_default():
    graph = init_graph(Graph())
    spec = IndexSpecBuilder(graph).build()

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("p1") == 5
    f2 = filter.Node.property("x") == True
    assert sorted(search_nodes(graph, f1 & f2)) == ["pometry"]

    f1 = filter.Edge.property("e_p1") < 5.0
    f2 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1 | f2)) == sorted(
        ["pometry->raphtory", "raphtory->pometry"]
    )


def test_mixed_node_and_edge_props_index_spec():
    graph = init_graph(Graph())
    spec = (
        IndexSpecBuilder(graph)
        .with_const_node_props(["x"])
        .with_all_temp_node_props()
        .with_all_edge_props()
        .build()
    )

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("p1") == 5
    f2 = filter.Node.property("y") == False
    assert sorted(search_nodes(graph, f1 | f2)) == sorted(["pometry", "raphtory"])

    f1 = filter.Edge.property("e_p1") < 5.0
    f2 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1 | f2)) == sorted(
        ["pometry->raphtory", "raphtory->pometry"]
    )


def test_get_index_spec():
    graph = init_graph(Graph())
    spec = (
        IndexSpecBuilder(graph)
        .with_const_node_props(["x"])
        .with_all_temp_node_props()
        .with_all_edge_props()
        .build()
    )

    graph.create_index_in_ram_with_spec(spec)

    returned_spec = graph.get_index_spec()

    node_const_names = {name for name in returned_spec.node_const_props}
    node_temp_names = {name for name in returned_spec.node_temp_props}
    edge_const_names = {name for name in returned_spec.edge_const_props}
    edge_temp_names = {name for name in returned_spec.edge_temp_props}

    assert "x" in node_const_names
    assert "p1" in node_temp_names or "p2" in node_temp_names
    assert "e_x" in edge_const_names or "e_y" in edge_const_names
    assert "e_p1" in edge_temp_names or "e_p2" in edge_temp_names


def test_const_prop_fallback_when_const_prop_indexed():
    graph = init_graph(Graph())
    spec = (
        IndexSpecBuilder(graph)
        .with_const_node_props(["x"])
        .with_const_edge_props(["e_y"])
        .build()
    )

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("x") == True
    assert sorted(search_nodes(graph, f1)) == sorted(["pometry"])

    f1 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1)) == sorted(["raphtory->pometry"])


def test_const_prop_fallback_when_const_prop_not_indexed():
    graph = init_graph(Graph())
    spec = (
        IndexSpecBuilder(graph)
        .with_all_temp_node_props()
        .with_all_temp_edge_props()
        .build()
    )

    graph.create_index_in_ram_with_spec(spec)

    f1 = filter.Node.property("x") == True
    assert sorted(search_nodes(graph, f1)) == sorted(["pometry"])

    f1 = filter.Edge.property("e_y") == False
    assert sorted(search_edges(graph, f1)) == sorted(["raphtory->pometry"])
