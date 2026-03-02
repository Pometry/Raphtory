## Graph Creation & Basics
from raphtory import Graph

g = Graph()
print(g)

# Add nodes with timestamps and properties
g.add_node(1, "alice", properties={"age": 30})
g.add_node(2, "bob", properties={"age": 25})
print(g.count_nodes())

# Add edges with layers
g.add_edge(3, "alice", "bob", properties={"weight": 1.0}, layer="friends")
print(g.count_edges())
