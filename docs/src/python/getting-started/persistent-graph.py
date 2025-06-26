# --8<-- [start:simple_graph]
from raphtory import PersistentGraph

G = PersistentGraph()

# new friendship
G.add_edge(1, "Alice", "Bob")

# additional friend
G.add_edge(3, "Bob", "Charlie")

# a dispute
G.delete_edge(5, "Alice", "Bob")

# a resolution
G.add_edge(10, "Alice", "Bob")

print(f"G's edges are {G.edges}")
print(f"G's exploded edges are {G.edges.explode()}")
# --8<-- [end:simple_graph]

# --8<-- [start:hanging_deletions]
G = PersistentGraph()

G.delete_edge(5, "Alice", "Bob")
print(f"G's edges are {G.edges.explode()}")
G.add_edge(1, "Alice", "Bob")
print(f"G's edges are {G.edges.explode()}")
# --8<-- [end:hanging_deletions]

# --8<-- [start:behaviour_1]
G = PersistentGraph()

G.add_edge(1, "Alice", "Bob")
G.delete_edge(5, "Alice", "Bob")
G.add_edge(3, "Alice", "Bob")
G.delete_edge(7, "Alice", "Bob")

print(G.edges.explode())
# --8<-- [end:behaviour_1]

# --8<-- [start:behaviour_2]
G1 = PersistentGraph()

G1.add_edge(1, 1, 2, properties={"message":"hi"})
G1.delete_edge(1, 1, 2)

print(f"G1's edges are {G1.edges.explode()}")

# --8<-- [end:behaviour_2]

# --8<-- [start:behaviour_3]
G = PersistentGraph()

G.add_edge(1, "Alice", "Bob", layer="colleagues")
G.delete_edge(5, "Alice", "Bob", layer="colleagues")
G.add_edge(3, "Alice", "Bob", layer ="friends")
G.delete_edge(7, "Alice", "Bob", layer="friends")

print(G.edges.explode())
# --8<-- [end:behaviour_3]

# --8<-- [start:at_1]
G = PersistentGraph()

G.add_edge(2, "Alice", "Bob")
G.delete_edge(5, "Alice", "Bob")

# before the edge is added
print(f"At time 0: {G.at(0).nodes} {G.at(0).edges.explode()}")

# at the instant the edge is added
print(f"At time 2: {G.at(2).nodes} {G.at(2).edges.explode()}")

# while the graph is active
print(f"At time 3: {G.at(3).nodes} {G.at(3).edges.explode()}")

# the instant the edge is deleted
print(f"At time 5: {G.at(5).nodes} {G.at(5).edges.explode()}")

# after the edge is deleted
print(f"At time 6: {G.at(6).nodes} {G.at(6).edges.explode()}")
# --8<-- [end:at_1]

# --8<-- [start:before_1]
G = PersistentGraph()

G.add_edge(2, "Alice", "Bob")
G.delete_edge(5, "Alice", "Bob")

# before the edge is added
print(f"Before time 1: {G.before(1).nodes} {G.before(1).edges.explode()}")

# at the instant the edge is added
print(f"Before time 2: {G.before(2).nodes} {G.before(2).edges.explode()}")

# while the graph is active
print(f"Before time 3: {G.before(3).nodes} {G.before(3).edges.explode()}")

# the instant the edge is deleted
print(f"Before time 5: {G.before(5).nodes} {G.before(5).edges.explode()}")

# after the edge is deleted
print(f"Before time 6: {G.before(6).nodes} {G.before(6).edges.explode()}")
# --8<-- [end:before_1]

# --8<-- [start:after_1]
G = PersistentGraph()

G.add_edge(2, "Alice", "Bob")
G.delete_edge(5, "Alice", "Bob")

# before the edge is added
print(f"After time 1: {G.after(1).nodes} {G.after(1).edges.explode()}")

# at the instant the edge is added
print(f"After time 2: {G.after(2).nodes} {G.after(2).edges.explode()}")

# while the graph is active
print(f"After time 3: {G.after(3).nodes} {G.after(3).edges.explode()}")

# the instant the edge is deleted
print(f"After time 5: {G.after(5).nodes} {G.after(5).edges.explode()}")

# after the edge is deleted
print(f"After time 6: {G.after(6).nodes} {G.after(6).edges.explode()}")
# --8<-- [end:after_1]

# --8<-- [start:window_1]
G = PersistentGraph()

G.add_edge(2, "Alice", "Bob")
G.delete_edge(5, "Alice", "Bob")

# Touching the start time of the edge
print(f"Window 0,2: {G.window(0,2).nodes} {G.window(0,2).edges.explode()}")

# Overlapping the start of the edge
print(f"Window 0,4: {G.window(0,4).nodes} {G.window(0,4).edges.explode()}")

# Fully inside the edge time
print(f"Window 3,4: {G.window(3,4).nodes} {G.window(3,4).edges.explode()}")

# Touching the end of the edge
print(f"Window 5,8: {G.window(5,8).nodes} {G.window(5,8).edges.explode()}")

# Fully containing the edge
print(f"Window 1,8: {G.window(1,8).nodes} {G.window(1,8).edges.explode()}")

# after the edge is deleted
print(f"Window 6,10: {G.window(6,10).nodes} {G.window(6,10).edges.explode()}")
# --8<-- [end:window_1]