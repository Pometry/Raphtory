# Filtering

A filter picks out part of a graph: the nodes with a high score, the edges that start at a given
node, the updates inside a time window. You describe the part you want as a
[filter expression][raphtory.filter.FilterExpr] and hand it to `filter()` on a graph, a node
collection or a node. The result is a view, so nothing is copied.

A filter expression is a small tree: *read something* (a name, a degree, a property), *compare it*
to a value or to another read, and *combine* comparisons with `&`, `|` and `~`. The same tree runs
locally and is what a remote graph sends to a server, and `repr()` prints it, so what you see is
what runs.

The examples below use this graph:

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph, filter

g = Graph()
g.add_node(0, "alice", properties={"score": 3.0})
g.add_node(2, "alice", properties={"score": 7.0})
g.add_node(1, "bob", properties={"score": 5.0})
g.add_node(0, "carol")
g.add_edge(1, "alice", "bob", layer="knows")
g.add_edge(2, "bob", "carol", layer="works")
```
///

## Where a filter starts

Every expression starts from one of four entry points. The entry point says what kind of thing is
being tested, and the rest of the expression is checked against it as you build it.

| start with | tests | example |
|---|---|---|
| [filter.Node][raphtory.filter.Node] | one node at a time | `filter.Node.property("score") > 4` |
| [filter.Edge][raphtory.filter.Edge] | one edge at a time | `filter.Edge.src().name() == "alice"` |
| [filter.ExplodedEdge][raphtory.filter.ExplodedEdge] | one edge update at a time | `filter.ExplodedEdge.property("weight") > 1` |
| [filter.Graph][raphtory.filter.Graph] | nothing; it is a view (window, layer, snapshot) | `filter.Graph.window(0, 2)` |

## What you can read

From a node, or from the end of an edge (`filter.Edge.src()` and `filter.Edge.dst()`):

| read | gives |
|---|---|
| `.name()`, `.id()`, `.node_type()` | the built-in fields |
| `.degree()`, `.in_degree()`, `.out_degree()` | how many neighbours the node has (nodes only, not edge ends) |
| `.property("score")` | the latest value of a temporal property |
| `.metadata("owner")` | a metadata (constant) value |

Edges and exploded edges read `.property(...)` and `.metadata(...)` too, and have yes/no tests of
their own: `.is_valid()`, `.is_deleted()`, `.is_active()`, `.is_self_loop()`.

## How you compare

A read is an [Expr][raphtory.filter.Expr]. Comparing it gives a `FilterExpr`.

| compare with | meaning |
|---|---|
| `==`, `!=`, `<`, `<=`, `>`, `>=` | the usual comparisons; the value must match the property's type family (a number for a number, a string for a string) |
| `.is_in([...])`, `.is_not_in([...])` | membership in a list of values |
| `.starts_with(s)`, `.ends_with(s)`, `.contains(s)`, `.not_contains(s)` | string tests |
| `.fuzzy_search(s, levenshtein_distance, prefix_match)` | approximate string match |
| `.is_some()`, `.is_none()` | whether the property has a value at all |

The right-hand side can be another read. `filter.Node.degree() > filter.Node.in_degree()` selects
nodes with a neighbour that does not point back at them.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
high = filter.Node.property("score") > 4
assert sorted(n.name for n in g.filter(high).nodes) == ["alice", "bob"]

missing = filter.Node.property("score").is_none()
assert [n.name for n in g.filter(missing).nodes] == ["carol"]

more_out_than_in = filter.Node.degree() > filter.Node.in_degree()
assert sorted(n.name for n in g.filter(more_out_than_in).nodes) == ["alice", "bob"]
```
///

## Combining filters

Use the bitwise operators: `&` for *and*, `|` for *or*, `~` for *not*. Python's `and`, `or` and
`not` do not work on filter expressions.

`~f` selects everything `f` did not select. A node without the property is not selected by
`property("score") > 4`, so it *is* selected by `~(property("score") > 4)`.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
assert [n.name for n in g.filter(~high).nodes] == ["carol"]

not_bob = high & ~(filter.Node.name() == "bob")
assert [n.name for n in g.filter(not_bob).nodes] == ["alice"]

either = (filter.Node.name() == "carol") | (filter.Node.property("score") > 6)
assert sorted(n.name for n in g.filter(either).nodes) == ["alice", "carol"]
```
///

## Reading through a view

A view can sit in front of a read. `filter.Node.window(0, 2).property("score")` reads the score
*as it was inside the window*, so alice's latest score there is 3, not 7. The same works for
`.layer(...)`, `.layers(...)`, `.latest()`, `.at(t)`, `.before(t)`, `.after(t)`, `.snapshot_at(t)`
and `.snapshot_latest()`, on nodes, edges and exploded edges, and they can be chained.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
early_high = filter.Node.window(0, 2).property("score") > 4
assert [n.name for n in g.filter(early_high).nodes] == ["bob"]
```
///

## Using a property's history

`.temporal()` switches a property read from its latest value to its whole history. An aggregate
then turns the history back into one value: `.sum()`, `.avg()`, `.min()`, `.max()`, `.first()`,
`.last()`, `.len()`. `.any()` and `.all()` ask whether the comparison holds for any, or every,
value in the history.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
total = filter.Node.property("score").temporal().sum() > 8
assert [n.name for n in g.filter(total).nodes] == ["alice"]

ever_low = filter.Node.property("score").temporal().any() < 4
assert [n.name for n in g.filter(ever_low).nodes] == ["alice"]
```
///

## Filtering edges

An edge filter can look at the edge itself or at either end of it.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
from_alice = filter.Edge.src().name() == "alice"
assert [(e.src.name, e.dst.name) for e in g.filter(from_alice).edges] == [("alice", "bob")]

in_works = filter.Edge.layer("works").is_active()
assert [(e.src.name, e.dst.name) for e in g.filter(in_works).edges] == [("bob", "carol")]
```
///

## Applying a filter

| call | what you get back |
|---|---|
| `graph.filter(expr)` | a graph view with only the matching nodes, or only the matching edges. A node filter keeps the edges between the remaining nodes; an edge filter keeps every node. |
| `graph.filter(filter.Graph.window(0, 2))` | the graph seen through the view; the same as `graph.window(0, 2)` |
| `graph.filter(filter.Graph.window(0, 2) & expr)` | the view first, then `expr` inside it: the same as `graph.window(0, 2).filter(expr)`. A view can be combined with `&` but not with `\|` or `~` |
| `graph.nodes.filter(expr)` | every node stays, but each node's edges and neighbours are narrowed to the ones that match |
| `node.filter(expr)` | the node with its edges and neighbours narrowed the same way |

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
assert g.filter(filter.Graph.window(0, 2)).count_edges() == 1

narrowed = g.nodes.filter(filter.Node.name() != "carol")
assert [n.name for n in narrowed] == ["alice", "bob", "carol"]
assert [n.degree() for n in narrowed] == [1, 1, 1]
```
///

## Seeing what a filter will do

`repr()` prints the tree. It is the same tree a remote graph sends, so there is no separate
server-side form to check.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
print(repr(filter.Node.window(0, 2).property("score") > 4))
```
///

!!! output

    ```
    FilterExpr(WINDOW[0..2](score) > 4)
    ```

## Cybersecurity scenario

Consider a cybersecurity team investigating the impact of a CVE on your company's servers. They
might use Raphtory to filter for nodes that are public-facing servers running a specific operating
system. This gives the security team a view that contains only the nodes that might be vulnerable.

Using the traffic dataset you can explore this scenario with `filter()`, which creates a new
`GraphView` containing only the nodes that match the CVE description:

/// tab | :fontawesome-brands-python: Python

```python
from raphtory import Graph
from raphtory import filter
import pandas as pd

server_edges_df = pd.read_csv("../data/network_traffic_edges.csv")
server_edges_df["timestamp"] = pd.to_datetime(server_edges_df["timestamp"])

server_nodes_df = pd.read_csv("../data/network_traffic_nodes.csv")
server_nodes_df["timestamp"] = pd.to_datetime(server_nodes_df["timestamp"])

traffic_graph = Graph()
traffic_graph.load_edges(
    data=server_edges_df,
    src="source",
    dst="destination",
    time="timestamp",
    properties=["data_size_MB"],
    layer_col="transaction_type",
    metadata=["is_encrypted"],
    shared_metadata={"datasource": "../data/network_traffic_edges.csv"},
)
traffic_graph.load_nodes(
    data=server_nodes_df,
    id="server_id",
    time="timestamp",
    properties=["OS_version", "primary_function", "uptime_days"],
    metadata=["server_name", "hardware_type"],
    shared_metadata={"datasource": "../data/network_traffic_edges.csv"},
)

my_filter = filter.Node.property("OS_version").is_in(["Ubuntu 20.04", "Red Hat 8.1"]) & filter.Node.property("primary_function").is_in(["Web Server", "Application Server"])

cve_view = traffic_graph.filter(my_filter)

print(cve_view.nodes)

```
///

You can print the nodes in the filtered view to see which machines you should investigate.

!!! output

    ```
    Nodes(Node(name=ServerB, earliest_time=1693555500000, latest_time=1693555800000, properties=Properties({OS_version: Red Hat 8.1, primary_function: Web Server, uptime_days: 45})), Node(name=ServerD, earliest_time=1693555800000, latest_time=1693556100000, properties=Properties({OS_version: Ubuntu 20.04, primary_function: Application Server, uptime_days: 60})))
    ```

```{.python continuation hide}
assert len(cve_view.nodes) == 2
```
