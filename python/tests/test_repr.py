from raphtory import Graph, PersistentGraph

# event graph with no layers
G = Graph()
G.add_edge(1, "A", "B")
G.add_edge(1, "A", "B")
print(G.edge("A","B"))

# event graph with two layers
G = Graph()
G.add_edge(1, "A", "B", layer = "layer 1")
G.add_edge(1, "A", "B", layer = "layer 2")
print(G.edge("A","B"))

# event graph with two layers and properties
G = Graph()
G.add_edge(1, "A", "B", layer = "layer 1", properties={"greeting":"howdy"})
G.add_edge(2, "A", "B", layer = "layer 2", properties={"greeting":"yo"})
print(G.edge("A","B"))
print(G.edge("A","B").explode())

# event graph with one layer and one non-layer
G = Graph()
G.add_edge(1, "A", "B", layer = "layer 1", properties={"greeting":"howdy"})
G.add_edge(2, "A", "B", properties={"greeting":"yo"})
print(G.edge("A","B"))

# persistent graph with layers
G = PersistentGraph()
G.add_edge(1, "A", "B", layer = "layer 1", properties={"greeting":"howdy"})
G.delete_edge(5, "A", "B", layer = "layer 1")
G.add_edge(2, "A", "B", layer = "layer 2", properties={"greeting":"yo"})
G.delete_edge(6, "A", "B", layer = "layer 2")
print(G.edge("A","B"))