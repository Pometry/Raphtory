# Views on a persistent graph

<script
  src="https://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML"
  type="text/javascript">
</script>

When dealing with *link-stream* graphs where edges are formed from instantaneous event streams, views were used to create a temporal bound on the graph to ultimately see how the graph changes over time. Single views were created using `at`, `before`, `after` and `window`, and iterators of windows were created using `expanding` and `rolling`.

Functionality with the same name is available for the *PersistentGraph*. This shares similarities with the functionality for *link-stream* graphs but has some important differences. This page covers the differences in time-bounding behavior on the Raphtory `PersistentGraph`.

## Querying an instant of the graph with `at()`

/// tab | :fontawesome-brands-python: Python
```python
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
```
///

!!! Output

    ```output
    At time 0: Nodes() Edges()
    At time 2: Nodes(Node(name=Alice, earliest_time=2, latest_time=2), Node(name=Bob, earliest_time=2, latest_time=2)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=3, layer(s)=[_default]))
    At time 3: Nodes(Node(name=Alice, earliest_time=3, latest_time=3), Node(name=Bob, earliest_time=3, latest_time=3)) Edges(Edge(source=Alice, target=Bob, earliest_time=3, latest_time=4, layer(s)=[_default]))
    At time 5: Nodes() Edges()
    At time 6: Nodes() Edges()
    ```

As we can see, the edge's presence in the graph is _inclusive_ of the timestamp at which it was added, but _exclusive_ of the timestamp at which it was deleted. Equivalently, it is present on a interval \\(1 \leq t < 5 \subseteq \mathbb{Z}\\). The earliest and latest times for each edge is adjusted to the time bound in the query.

While nodes are not present until they are added (see example at time 1), once they are added they are in the graph forever (see example at time 6). This differs from the `Graph` equivalent where nodes are present only when they contain an update within the time bounds. 

Crucially, this means that while performing a node count on a `Graph` will count the nodes who have activity (a property update, an adjacent edge added) within the time bounds specified. The same is not true for `PersistentGraph`s.

## Getting the graph before a certain point with `before()`

/// tab | :fontawesome-brands-python: Python
```python
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
```
///

!!! Output

    ```output
    Before time 1: Nodes() Edges()
    Before time 2: Nodes() Edges()
    Before time 3: Nodes(Node(name=Alice, earliest_time=2, latest_time=2), Node(name=Bob, earliest_time=2, latest_time=2)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=3, layer(s)=[_default]))
    Before time 5: Nodes(Node(name=Alice, earliest_time=2, latest_time=2), Node(name=Bob, earliest_time=2, latest_time=2)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=5, layer(s)=[_default]))
    Before time 6: Nodes(Node(name=Alice, earliest_time=2, latest_time=5), Node(name=Bob, earliest_time=2, latest_time=5)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=5, layer(s)=[_default]))
    ```

Here we see that the `before(T)` bound is exclusive of the end point \\(T\\), creating an intersection between the time interval \\(-\infty < t < T\\) and \\(2 \leq t < 5\\) where \\(T\\) is the argument of `before`.

## Getting the graph after a certain point with `after()`

/// tab | :fontawesome-brands-python: Python
```python
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
```
///

!!! Output

    ```output
    After time 1: Nodes(Node(name=Alice, earliest_time=2, latest_time=5), Node(name=Bob, earliest_time=2, latest_time=5)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=5, layer(s)=[_default]))
    After time 2: Nodes(Node(name=Alice, earliest_time=3, latest_time=5), Node(name=Bob, earliest_time=3, latest_time=5)) Edges(Edge(source=Alice, target=Bob, earliest_time=3, latest_time=5, layer(s)=[_default]))
    After time 3: Nodes(Node(name=Alice, earliest_time=4, latest_time=5), Node(name=Bob, earliest_time=4, latest_time=5)) Edges(Edge(source=Alice, target=Bob, earliest_time=4, latest_time=5, layer(s)=[_default]))
    After time 5: Nodes() Edges()
    After time 6: Nodes() Edges()
    ```

`after(T)` is also exclusive of the starting point \\(T\\).

## Windowing the graph with `window()`

/// tab | :fontawesome-brands-python: Python
```python
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
```
///

!!! Output

    ```output
    Window 0,2: Nodes() Edges()
    Window 0,4: Nodes(Node(name=Alice, earliest_time=2, latest_time=2), Node(name=Bob, earliest_time=2, latest_time=2)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=4, layer(s)=[_default]))
    Window 3,4: Nodes(Node(name=Alice, earliest_time=3, latest_time=3), Node(name=Bob, earliest_time=3, latest_time=3)) Edges(Edge(source=Alice, target=Bob, earliest_time=3, latest_time=4, layer(s)=[_default]))
    Window 5,8: Nodes() Edges()
    Window 1,8: Nodes(Node(name=Alice, earliest_time=2, latest_time=5), Node(name=Bob, earliest_time=2, latest_time=5)) Edges(Edge(source=Alice, target=Bob, earliest_time=2, latest_time=5, layer(s)=[_default]))
    Window 6,10: Nodes() Edges()
    ```

A `window(T1, T2)` creates a half-open interval \\(T_1 \leq t < T_2\\) intersecting the edge's active time ( \\(2 \leq t < 5 \\) in this case). When the window is completely inside the edge active time and when the edge's active time is strictly inside the window. In both cases, the edge is treated as present in the graph.