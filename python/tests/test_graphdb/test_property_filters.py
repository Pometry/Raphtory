from raphtory import Graph, PersistentGraph, Prop


def build_graph():
    graph = Graph()
    graph.add_edge(0, 1, 2, {"test_str": "first", "test_int": 0})
    graph.add_edge(1, 2, 3, {"test_str": "second", "test_int": 1})
    graph.add_edge(2, 3, 4, {"test_int": 2})
    graph.add_edge(3, 3, 4, {"test_int": 3})
    graph.add_edge(4, 2, 3, {"test_bool": True})
    graph.add_edge(5, 2, 3, {"test_str": "third"})
    return graph


def test_filter_edges():
    graph = build_graph()

    assert graph.filter_edges(Prop("test_str") == "first").edges.id == [(1, 2)]
    # is this the answer we want?, currently excludes edges that don't have the property
    assert graph.filter_edges(Prop("test_str") != "first").edges.id == [(2, 3)]
    assert graph.filter_edges(Prop("test_str").is_some()).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_edges(Prop("test_str").is_none()).edges.id == [(3, 4)]
    assert graph.filter_edges(Prop("test_str") == "second").edges.id == []
    assert graph.before(5).filter_edges(Prop("test_str") == "second").edges.id == [(2, 3)]
    assert graph.filter_edges(Prop("test_str").any({"first", "fourth"})).edges.id == [(1, 2)]
    assert graph.filter_edges(Prop("test_str").not_any({"first"})).edges.id == [(2, 3), (3, 4)]

    assert graph.filter_edges(Prop("test_int") == 2).edges.id == []  # only looks at the latest value
    assert graph.filter_edges(Prop("test_int") != 1).edges.id == [(1, 2), (3, 4)]
    assert graph.filter_edges(Prop("test_int") > 2).edges.id == [(3, 4)]
    assert graph.filter_edges(Prop("test_int") >= 1).edges.id == [(2, 3), (3, 4)]
    assert graph.filter_edges(Prop("test_int") < 3).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_edges(Prop("test_int") <= 1).edges.id == [(1, 2), (2, 3)]

    assert graph.filter_edges(Prop("test_bool") == True).edges.id == [(2, 3)]  # worth adding special support for this?


def test_filter_exploded_edges():
    graph = build_graph()

    assert graph.filter_exploded_edges(Prop("test_str") == "first").edges.id == [(1, 2)]
    # is this the answer we want?, currently excludes edges that don't have the property
    assert graph.filter_exploded_edges(Prop("test_str") != "first").edges.id == [(2, 3)]
    assert graph.filter_exploded_edges(Prop("test_str").is_some()).edges.id == [(1, 2), (2, 3)]
    assert graph.filter_exploded_edges(Prop("test_str").is_none()).edges.id == [(2, 3), (3, 4)]
    assert graph.filter_exploded_edges(Prop("test_str") == "second").edges.id == [(2, 3)]
    assert graph.filter_exploded_edges(Prop("test_str").any({"first", "fourth"})).edges.id == [(1, 2)]
    assert graph.filter_exploded_edges(Prop("test_str").not_any({"first"})).edges.id == [(2, 3), (3, 4)]

    assert graph.filter_exploded_edges(Prop("test_int") == 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") != 2).edges.id == [(1, 2), (2, 3), (3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") > 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") >= 2).edges.id == [(3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") < 3).edges.id == [(1, 2), (2, 3), (3, 4)]
    assert graph.filter_exploded_edges(Prop("test_int") <= 1).edges.id == [(1, 2), (2, 3)]

    assert graph.filter_exploded_edges(Prop("test_bool") == True).edges.id == [
        (2, 3)]  # worth adding special support for this?
