# Handling of ambiguous updates

For a *link-stream* graph there is a natural way to construct the graph regardless of the order the updates come in. However, for a *PersistentGraph* where deletions are possible this becomes more difficult. The following examples illustrate this.

/// tab | :fontawesome-brands-python: Python
```python
'foo'
```
///

/// tab | :fontawesome-brands-rust: Rust
```rust
'bar'
```
///

## Order of resolving additions and deletions

=== ":fontawesome-brands-python: Python"

    ```python
    G = PersistentGraph()

    G.add_edge(1, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    G.add_edge(3, "Alice", "Bob")
    G.delete_edge(7, "Alice", "Bob")

    print(G.edges.explode())
    ```

Here two edges between Alice and Bob overlap in time: one starting at time 1 and ending at time 5, another starting at time 3 and ending at time 7. 

For *link-stream* graphs in Raphtory are allowed to have edges between the same pair of nodes happening at the same instant. However, when we look at the exploded edges of this *PersistentGraph* graph, the following is returned:

!!! Output

    ```output
    Edges(Edge(source=Alice, target=Bob, earliest_time=1, latest_time=3, layer(s)=[_default]), Edge(source=Alice, target=Bob, earliest_time=3, latest_time=5, layer(s)=[_default]))
    ```

Two edges are created, one that exists from time 1 to time 3 and another that exists from time 3 to time 5. The second deletion at time 7 is ignored. 

The reason for this is that Raphtory's graph updates are inserted in chronological order, so that the same graph is constructed regardless of the order in which the updates are made. With an exception for events which have the same timestamp, which will be covered shortly. 

In this example, the order is: edge addition at time 1, edge addition at time 3, edge deletion at time 5 and edge deletion at time 7. This second edge deletion is now redundant.

## Hanging deletions

Adding edges without a deletion afterwards results in an edge which lasts forever, while deleting an edge without a prior addition does not effect the history. However, hanging deletions are tracked as an object and if the history is later modified to add the corresponding edge at an earlier time the delete will become valid and occur as expected.

=== ":fontawesome-brands-python: Python"

    ```python
    G = PersistentGraph()

    G.delete_edge(5, "Alice", "Bob")
    print(f"G's edges are {G.edges.explode()}")
    G.add_edge(1, "Alice", "Bob")
    print(f"G's edges are {G.edges.explode()}")
    ```

Which results in the following:
!!! Output

    ```output
    G's edges are Edges()
    G's edges are Edges(Edge(source=Alice, target=Bob, earliest_time=1, latest_time=5, layer(s)=[_default]))
    ```

## Additions and deletions in the same instant

If the update times to an edge are all distinct from each other, the graph that is constructed is fully unambiguous. When events have the same timestamp Raphtory tie-breaks the updates by the order in which they are executed.

In the following, it is impossible to infer what the intended update order is so a tie-break is required.

=== ":fontawesome-brands-python: Python"

    ```python
    G1 = PersistentGraph()

    G1.add_edge(1, 1, 2, properties={"message":"hi"})
    G1.delete_edge(1, 1, 2)

    print(f"G1's edges are {G1.edges.explode()}")
    ```

!!! Output

    ```output
    G1's edges are Edges(Edge(source=1, target=2, earliest_time=1, latest_time=1, properties={message: hi}, layer(s)=[_default]))
    ```

This graph has an edge which instantaneously appears and disappears at time 1 and therefore the order of its history is determined by the execution order.

## Interaction with layers

Layering allows different types of interaction to exist, and edges on different layers can have overlapping times in a way that doesn't make sense for edges in the same layer or for edges with no layer. 

Consider an example without layers:

=== ":fontawesome-brands-python: Python"

    ```python
    G = PersistentGraph()

    G.add_edge(1, "Alice", "Bob")
    G.delete_edge(5, "Alice", "Bob")
    G.add_edge(3, "Alice", "Bob")
    G.delete_edge(7, "Alice", "Bob")

    print(G.edges.explode())
    ```

!!! Output

    ```output
    Edges(Edge(source=Alice, target=Bob, earliest_time=1, latest_time=3, layer(s)=[_default]), Edge(source=Alice, target=Bob, earliest_time=3, latest_time=5, layer(s)=[_default]))
    ```

Now take a look at a  modified example with layers:

=== ":fontawesome-brands-python: Python"

```python
G = PersistentGraph()

G.add_edge(1, "Alice", "Bob", layer="colleagues")
G.delete_edge(5, "Alice", "Bob", layer="colleagues")
G.add_edge(3, "Alice", "Bob", layer ="friends")
G.delete_edge(7, "Alice", "Bob", layer="friends")

print(G.edges.explode())
```

!!! Output

    ```output
    Edges(Edge(source=Alice, target=Bob, earliest_time=1, latest_time=5, layer(s)=[colleagues]), Edge(source=Alice, target=Bob, earliest_time=3, latest_time=7, layer(s)=[friends]))
    ```

By adding layer names to the different edge instances we produce a different result.

Here we have two edges, one starting and ending at 1 and 5 respectively with the 'colleague' layer, the other starting and ending at 3 and 7 on the 'friends' layer. 
