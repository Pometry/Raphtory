from raphtory import Graph, PersistentGraph
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
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=1, event_id=1), layer(s)=[_default])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # event graph with two layers
    def test_layers_no_props(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1")
        G.add_edge(1, "A", "B", layer="layer 2")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=1, event_id=1), layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # edge with more than 11 layers
    def test_many_layers(self):
        G = Graph()
        for i in range(20):
            G.add_edge(i, "A", "B", layer=f"layer {i}")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=0, event_id=0), latest_time=EventTime(epoch=19, event_id=19), layer(s)=[layer 0,layer 1,layer 2,layer 3,layer 4,layer 5,layer 6,layer 7,layer 8,layer 9, ...])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # event graph with two layers and properties
    def test_layers_and_props(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.add_edge(2, "A", "B", layer="layer 2", properties={"greeting": "yo"})
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=2, event_id=1), properties={greeting: yo}, layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

        expected_out = "Edges(Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=1, event_id=0), properties={greeting: howdy}, layer(s)=[layer 1]), Edge(source=A, target=B, earliest_time=EventTime(epoch=2, event_id=1), latest_time=EventTime(epoch=2, event_id=1), properties={greeting: yo}, layer(s)=[layer 2]))"
        self.assertEqual(repr(G.edge("A", "B").explode()), expected_out)

    # event graph with one layer and one non-layer
    def test_layers_and_non_layers(self):
        G = Graph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.add_edge(2, "A", "B", properties={"greeting": "yo"})
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=2, event_id=1), properties={greeting: yo}, layer(s)=[layer 1, _default])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)

    # persistent graph with layers
    def test_persistent_graph(self):
        G = PersistentGraph()
        G.add_edge(1, "A", "B", layer="layer 1", properties={"greeting": "howdy"})
        G.delete_edge(5, "A", "B", layer="layer 1")
        G.add_edge(2, "A", "B", layer="layer 2", properties={"greeting": "yo"})
        G.delete_edge(6, "A", "B", layer="layer 2")
        expected_out = "Edge(source=A, target=B, earliest_time=EventTime(epoch=1, event_id=0), latest_time=EventTime(epoch=6, event_id=3), properties={greeting: yo}, layer(s)=[layer 1, layer 2])"
        self.assertEqual(repr(G.edge("A", "B")), expected_out)


if __name__ == "__main__":
    unittest.main()
