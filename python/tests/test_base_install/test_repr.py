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
    """`repr` shows the wire form a filter carries, so what runs locally and what
    would be sent to a server can be read off the object."""

    def test_repr_shows_the_recorded_wire_form(self):
        expr = filter.Node.window(0, 5).property("score") > 4
        self.assertEqual(repr(expr), "FilterExpr(WINDOW[0..5](score > 4))")

    def test_repr_shows_temporal_ops_and_combinators(self):
        expr = (filter.Node.property("score").temporal().sum() > 10) & ~(
            filter.Node.name() == "carol"
        )
        self.assertEqual(
            repr(expr),
            "FilterExpr((sum(temporal(score)) > 10 AND NOT(node_name == carol)))",
        )

    def test_repr_of_a_local_only_filter_says_so(self):
        expr = filter.Node.degree() > filter.Node.in_degree()
        self.assertEqual(repr(expr), "FilterExpr(<local only: no server-side form>)")
