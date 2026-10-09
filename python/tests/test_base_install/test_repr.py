from raphtory import Graph, PersistentGraph
from raphtory import filter
from io import StringIO
import unittest
from unittest import TestCase
from unittest.mock import patch


class PyReprTest(TestCase):
    # event graph with no layers
    def test_no_layers(self):
        G = Graph()
        G.add_edge(1, "A", "B")
        G.add_edge(1, "A", "B")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=1), layer(s)=[_default])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # event graph with two layers
    def test_layers_no_props(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1")
        G.add_edge(1, "A", "B", layer="layer 2")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=1), layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # edge with more than 11 layers
    def test_many_layers(self):
        G = Graph()
        for i in range(20):
            G.add_edge(i, "A", "B", layer=f"layer {i}")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=0, dt=1970-01-01T00:00:00+00:00, event_id=0), latest_time=EventTime(t=19, dt=1970-01-01T00:00:00.019+00:00, event_id=19), layer(s)=[layer 0,layer 1,layer 2,layer 3,layer 4,layer 5,layer 6,layer 7,layer 8,layer 9, ...])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # event graph with two layers and properties
    def test_layers_and_props(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.add_edge(2, "A", "B", layer="layer 2", properties={"greeting": "yo"})
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=2, dt=1970-01-01T00:00:00.002+00:00, event_id=1), properties={greeting: yo}, layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

        expected_out = "Edges(Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), properties={greeting: howdy}, layer(s)=[layer 1]), Edge(source=A, target=B, earliest_time=EventTime(t=2, dt=1970-01-01T00:00:00.002+00:00, event_id=1), latest_time=EventTime(t=2, dt=1970-01-01T00:00:00.002+00:00, event_id=1), properties={greeting: yo}, layer(s)=[layer 2]))"
        self.assertEqual(repr(G.edge("A", "B").explode()), expected_out)

    # event graph with one layer and one non-layer
    def test_layers_and_non_layers(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.add_edge(2, "A", "B", properties={"greeting": "yo"})
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=2, dt=1970-01-01T00:00:00.002+00:00, event_id=1), properties={greeting: yo}, layer(s)=[layer 1, _default])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # persistent graph with layers
    def test_persistent_graph(self):
        G = PersistentGraph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.delete_edge(5, "A", "B", layer="layer 1")
        G.add_edge(2, "A", "B", layer="layer 2", properties={"greeting": "yo"})
        G.delete_edge(6, "A", "B", layer="layer 2")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(t=1, dt=1970-01-01T00:00:00.001+00:00, event_id=0), latest_time=EventTime(t=6, dt=1970-01-01T00:00:00.006+00:00, event_id=3), properties={greeting: yo}, layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)


if __name__ == "__main__":
    unittest.main()


class FilterExprReprTest(TestCase):
    """`repr` is the Python that builds the expression, module-qualified, so
    `eval` rebuilds it after `import raphtory`."""

    def test_repr_is_the_python_that_builds_the_expression(self):
        expr = filter.Node.window(0, 5).property("score") > 4
        self.assertEqual(
            repr(expr), "raphtory.filter.Node.window(0, 5).property('score') > 4"
        )

    def test_repr_shows_temporal_ops_and_combinators(self):
        expr = (filter.Node.property("score").temporal().sum() > 10) & ~(
            filter.Node.name() == "carol"
        )
        self.assertEqual(
            repr(expr),
            "(raphtory.filter.Node.property('score').temporal().sum() > 10)"
            " & ~(raphtory.filter.Node.name() == 'carol')",
        )

    def test_repr_shows_layer_exclusion_default_layer_and_shrinks(self):
        expr = (
            filter.Graph.default_layer()
            .exclude_layer("a")
            .exclude_layers(["b", "c"])
            .shrink_start(2)
            .shrink_end(9)
        )
        self.assertEqual(
            repr(expr),
            "raphtory.filter.Graph.default_layer().exclude_layer('a')"
            ".exclude_layers(['b', 'c']).shrink_start(2).shrink_end(9)",
        )

    def test_repr_shows_node_set_views_and_valid(self):
        expr = (
            filter.Graph.exclude_nodes(["a", 7])
            .subgraph([1, "b"])
            .subgraph_node_types(["person", "org"])
            .valid()
        )
        self.assertEqual(
            repr(expr),
            "raphtory.filter.Graph.exclude_nodes(['a', 7]).subgraph([1, 'b'])"
            ".subgraph_node_types(['person', 'org']).valid()",
        )

    def test_repr_shows_expressions_on_both_sides(self):
        expr = filter.Node.degree() > filter.Node.in_degree()
        self.assertEqual(
            repr(expr),
            "raphtory.filter.Node.degree() > raphtory.filter.Node.in_degree()",
        )

    def test_repr_round_trips_through_eval(self):
        import raphtory

        cases = [
            filter.Node.window(0, 5).property("score") > 4,
            filter.Node.property("p").temporal().starts_with("Go").all(),
            filter.Node.layer("work").property("p").is_in(["a", "b", 3])
            | filter.Node.metadata("m").is_some(),
            (filter.Node.name() == "a")
            & (filter.Node.name() == "b")
            & (filter.Node.name() == "c"),
            filter.Edge.window(1, 4).src().property("p").temporal().len() >= 2,
            filter.Edge.dst().name().fuzzy_search("bob", 1, True),
            filter.Edge.is_valid() & filter.Edge.layers(["a", "b"]).is_active(),
            filter.ExplodedEdge.property("p") == 3.5,
            filter.Graph.window(0, 5).latest(),
            filter.Graph.window(0, 5) & (filter.Node.name() != "x"),
            filter.Node.property("s") == "it's",
            filter.Node.window((3, 2), 9).is_active(),
            filter.Node.property("p"),
            filter.Edge.at(3).src(),
            filter.Graph.default_layer().exclude_layer("a"),
            filter.Graph.exclude_layers(["a", "b"]).shrink_start(2).shrink_end(9),
            filter.Node.shrink_start(2).exclude_layer("a").property("p") > 1,
            filter.Edge.window(1, 9).shrink_end((5, 1)).default_layer().is_active(),
            filter.ExplodedEdge.exclude_layers(["a", "b"]).property("p") == 3.5,
            filter.Graph.exclude_nodes(["a", 7]).valid(),
            filter.Node.subgraph([1, "it's"]).degree() > 0,
            filter.Edge.subgraph_node_types(["t"]).valid().is_active(),
            filter.ExplodedEdge.exclude_nodes([]).property("p") == 3.5,
        ]
        for expr in cases:
            text = repr(expr)
            self.assertEqual(repr(eval(text, {"raphtory": raphtory})), text)
