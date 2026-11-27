from raphtory import filter
from filters_setup import init_graph, create_test_graph
from utils import with_variants


@with_variants(init_graph, variants=["graph"])
def test_edges_src_property_eq():
    def check(graph):
        expr = filter.Edge.src().property("p2") == 2
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "1"), ("2", "3")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_dst_property_contains():
    def check(graph):
        # dst node "3" has p20 == "Gold_boat"; dst node "4" has p20 updated from Gold_boat to Gold_ship so it doesn't appear here
        expr = filter.Edge.dst().property("p20").contains("boat")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_dst_property_any_contains():
    def check(graph):
        # dst node "3" has p20 == "Gold_boat"; dst node "4" has p20 updated from Gold_boat to Gold_ship so it appears with .temporal().any()
        expr = filter.Edge.dst().property("p20").temporal().contains("boat").any()
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3"), ("3", "4")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_dst_property_gt():
    def check(graph):
        expr = filter.Edge.dst().property("p100") > 55
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted(
            [("2", "3")]
        )  # edge (2,3) is at times 2 and 3; IDs dedup by view
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_property_temporal_sum():
    def check(graph):
        expr = filter.Edge.src().property("prop6").temporal().latest().sum() == 12
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_property_any_equals():
    def check(graph):
        # src node "d" doesn't exist as src; src of edges are a,b,c; node "a" has prop8 = [2,3,3]
        expr = (filter.Edge.src().property("prop8").temporal() == 3).any().any()
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_metadata_sum_with_same_name_property():
    def check(graph):
        # src node "a" metadata prop1 sum == 36
        # src node "b" and "c" property prop1 < 36, they don't appear because it's property not metadata
        expr = filter.Edge.src().metadata("prop1").sum() <= 36
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_metadata_avg():
    def check(graph):
        expr = filter.Edge.src().metadata("prop2").avg() <= 2.0
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d"), ("b", "d")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_src_property_and_edge_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) & (
            filter.Edge.property("p20").temporal().contains("ship").any()
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_src_and_dst_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) & (
            filter.Edge.dst().property("p20").contains("boat")
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_src_or_dst_property():
    def check(graph):
        expr = (filter.Edge.src().property("p2") == 2) | (
            filter.Edge.dst().property("p20").contains("boat")
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "1"), ("2", "3")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_property_and_dst_name():
    def check(graph):
        expr = (filter.Edge.src().property("prop1") >= 20) & (
            filter.Edge.dst().name() == "d"
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d"), ("c", "d")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_metadata_and_edge_property():
    def check(graph):
        expr = (filter.Edge.src().metadata("prop1").sum() == 36) & (
            filter.Edge.property("eprop1") > 20
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected

    return check


# node_type endpoint filters
@with_variants(init_graph, variants=["graph"])
def test_edges_src_node_type_eq():
    def check(graph):
        expr = filter.Edge.src().node_type() == "fire_nation"
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("1", "2"), ("3", "1"), ("3", "4")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_dst_node_type_contains():
    def check(graph):
        expr = filter.Edge.dst().node_type().contains("nomads")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("1", "2")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_src_and_dst_node_type():
    def check(graph):
        expr = filter.Edge.src().node_type().starts_with(
            "fire"
        ) & filter.Edge.dst().node_type().contains("nomads")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("1", "2")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_src_or_dst_node_type():
    def check(graph):
        expr = (filter.Edge.src().node_type() == "air_nomads") | (
            filter.Edge.dst().node_type() == "fire_nation"
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("2", "3"), ("2", "1"), ("3", "1")])
        assert result == expected

    return check


@with_variants(init_graph, variants=["graph"])
def test_edges_not_src_node_type():
    def check(graph):
        expr = ~filter.Edge.src().node_type().contains("fire")
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted(
            [
                ("2", "3"),
                ("2", "1"),
                ("David Gilmour", "John Mayer"),
                ("John Mayer", "Jimmy Page"),
            ]
        )
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_edges_src_node_type_eq_compose_with_edge_prop():
    def check(graph):
        # edge ("c","d") has eprop1 > 20 but src node_type isn't fire_nation, so not included here
        expr = (filter.Edge.src().node_type() == "fire_nation") & (
            filter.Edge.property("eprop1") > 20
        )
        result = sorted(graph.filter(expr).edges.id)
        expected = sorted([("a", "d")])
        assert result == expected

    return check


@with_variants(create_test_graph)
def test_an_endpoint_takes_every_node_term():
    """An edge passes `src().term` when its source passes `term` on nodes, and
    `dst().term` when its destination does, inside the same views."""

    def check(graph):
        Node, Edge = filter.Node, filter.Edge
        cases = {
            "degree": (
                Node.degree() > 1,
                Edge.src().degree() > 1,
                Edge.dst().degree() > 1,
            ),
            "in_degree": (
                Node.in_degree() >= 1,
                Edge.src().in_degree() >= 1,
                Edge.dst().in_degree() >= 1,
            ),
            "out_degree": (
                Node.out_degree() >= 2,
                Edge.src().out_degree() >= 2,
                Edge.dst().out_degree() >= 2,
            ),
            "is_active": (
                Node.window(1, 3).is_active(),
                Edge.window(1, 3).src().is_active(),
                Edge.window(1, 3).dst().is_active(),
            ),
            "exists": (
                Node.window(1, 3).exists(),
                Edge.window(1, 3).src().exists(),
                Edge.window(1, 3).dst().exists(),
            ),
        }
        every = sorted(graph.edges.id)
        for term, (node, src, dst) in cases.items():
            passing = set(graph.nodes[node].id)
            assert sorted(graph.edges[src].id) == [
                e for e in every if e[0] in passing
            ], f"src {term}"
            assert sorted(graph.edges[dst].id) == [
                e for e in every if e[1] in passing
            ], f"dst {term}"

    return check
