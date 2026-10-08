# Filtering

A filter picks out part of a graph: the nodes with a high score, the edges that start at a given
node, the updates inside a time window. You describe the part you want as a
[filter expression][raphtory.filter.FilterExpr] and hand it to `filter()` on a graph, a node
collection or a node. The result is a view, so nothing is copied.

A filter expression is a small tree: a *term* (a name, a degree, a property), *compared* to a
literal or to another term, and *combined* with `&`, `|` and `~`. A term is a value taken from the
node or edge under test; a literal is a constant you write. The same tree runs locally and is what
a remote graph sends to a server, and `repr()` prints it, so what you see is what runs.

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

## Terms

From a node, or from the end of an edge (`filter.Edge.src()` and `filter.Edge.dst()`):

| term | gives |
|---|---|
| `.name()`, `.id()`, `.node_type()` | the built-in fields |
| `.degree()`, `.in_degree()`, `.out_degree()` | how many neighbours the node has (nodes only, not edge ends) |
| `.property("score")` | the latest value of a temporal property |
| `.metadata("owner")` | a metadata (constant) value |

Edges and exploded edges offer `.property(...)` and `.metadata(...)` too, and have yes/no tests of
their own: `.is_valid()`, `.is_deleted()`, `.is_active()`, `.is_self_loop()`.

## How you compare

A term is an [Expr][raphtory.filter.Expr]. Comparing it gives a yes/no `Expr`, which is accepted anywhere a filter is.

| compare with | meaning |
|---|---|
| `==`, `!=`, `<`, `<=`, `>`, `>=` | the usual comparisons; the value must match the property's type family (a number for a number, a string for a string) |
| `.is_in([...])`, `.is_not_in([...])` | membership in a list of values |
| `.starts_with(s)`, `.ends_with(s)`, `.contains(s)`, `.not_contains(s)` | string tests |
| `.fuzzy_search(s, levenshtein_distance, prefix_match)` | approximate string match |
| `.is_some()`, `.is_none()` | whether the property has a value at all |

The right-hand side can be another term. `filter.Node.degree() > filter.Node.in_degree()` selects
nodes with a neighbour that does not point back at them.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
high = filter.Node.property("score") > 4
assert sorted(n.name for n in g.filter(high).nodes) == ["alice", "bob"]

missing = filter.Node.property("score").is_none()
assert [n.name for n in g.filter(missing).nodes] == ["carol"]

has_a_neighbour_that_does_not_point_back = filter.Node.degree() > filter.Node.in_degree()
assert sorted(n.name for n in g.filter(has_a_neighbour_that_does_not_point_back).nodes) == [
    "alice",
    "bob",
]
```
///

## Combining filters

Use the bitwise operators: `&` for *and*, `|` for *or*, `~` for *not*. Python's `and`, `or` and
`not` do not work on filter expressions.

`~f` selects everything `f` did not select. A node without the property is not selected by
`property("score") > 4`, so it *is* selected by `~(property("score") > 4)`. A negated node
predicate is still a node predicate: on edges it keeps the edges between the nodes that fail it.
Negating a combination negates its node tests and its edge tests: `~(a & b)` of two node tests is
`~a | ~b`, and `~(node_test & edge_test)` keeps the nodes that fail the node test and, between
them, the edges that fail the edge test.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
assert [n.name for n in g.filter(~high).nodes] == ["carol"]

not_bob = high & ~(filter.Node.name() == "bob")
assert [n.name for n in g.filter(not_bob).nodes] == ["alice"]

either = (filter.Node.name() == "carol") | (filter.Node.property("score") > 6)
assert sorted(n.name for n in g.filter(either).nodes) == ["alice", "carol"]
```
///

## Selecting from a collection

`collection[f]` keeps the items of the collection on its left that pass `f`, and asks the question
once per item: once per edge on `g.edges`, once per exploded edge on `g.edges.explode()`. The
kind of `f` does not change that. A view used this way, `g.edges[filter.Graph.window(0, 5)]`, keeps
the edges that exist in the window, and exploding them afterwards lists all of their updates; to see
only the updates inside the window, put the window on the graph: `g.window(0, 5).edges.explode()`.
Beside other legs, `g.edges[filter.Graph.window(0, 5) & f]`, the view is one test among the others:
the edges that exist in the window and pass `f` on `g` itself.

## Reading through a view

A view can sit in front of a term. `filter.Node.window(0, 2).property("score")` reads the score
*as it was inside the window*, so alice's latest score there is 3, not 7. The same works for
`.layer(...)`, `.layers(...)`, `.latest()`, `.at(t)`, `.before(t)`, `.after(t)`, `.snapshot_at(t)`
and `.snapshot_latest()`, on nodes, edges and exploded edges, and they can be chained.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
early_high = filter.Node.window(0, 2).property("score") > 4
assert [n.name for n in g.filter(early_high).nodes] == ["bob"]
```
///

Five more views choose layers or trim a window. Each one does what the graph method of the same
name does to the view built so far. The *default layer* holds the updates added without a layer
name.

| view | reads through | example |
|---|---|---|
| `.default_layer()` | the default layer only | `filter.Edge.default_layer().is_active()` |
| `.exclude_layer(name)` | every layer except `name` | `filter.Edge.exclude_layer("knows").is_active()` |
| `.exclude_layers([...])` | every layer except the ones listed | `filter.Node.exclude_layers(["knows", "works"]).degree() > 0` |
| `.shrink_start(t)` | the current window, with its start moved to `t` if `t` is later | `filter.Node.window(0, 3).shrink_start(2).property("score") > 4` |
| `.shrink_end(t)` | the current window, with its end moved to `t` if `t` is earlier | `filter.Node.shrink_end(2).property("score") > 4` |

The two shrinks only ever narrow a window. `window(0, 3).shrink_start(2)` reads through
`window(2, 3)`, where alice's latest score is 7 and bob has no score at all; a start before the
current one changes nothing. On a view with no window, `shrink_start(2)` reads from 2 onwards.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
not_knows = filter.Edge.exclude_layer("knows").is_active()
assert [(e.src.name, e.dst.name) for e in g.filter(not_knows).edges] == [("bob", "carol")]

late_high = filter.Node.window(0, 3).shrink_start(2).property("score") > 4
assert [n.name for n in g.filter(late_high).nodes] == ["alice"]
```
///

Four more views choose which nodes and edges are in the view. Like the others, each one does what
the graph method of the same name does to the view built so far. A node is named by its name or,
in a graph indexed by integers, by its integer id.

| view | reads through | example |
|---|---|---|
| `.exclude_nodes([...])` | every node except the ones listed, and the edges between the rest | `filter.Node.exclude_nodes(["carol"]).degree() > 0` |
| `.subgraph([...])` | only the nodes listed, and the edges between them | `filter.Graph.subgraph(["alice", "bob"])` |
| `.subgraph_node_types([...])` | only the nodes of the types listed, and the edges between them | `filter.Node.subgraph_node_types(["person"]).degree() > 1` |
| `.valid()` | only the edges that are valid: on a persistent graph, those whose last update is not a deletion | `filter.Edge.valid().is_active()` |

A name the view does not hold is skipped. A term read through one of these views for a node the
view leaves out answers as it does for a node outside a window: a property or a name has no value,
`.degree()` is 0 and `.is_active()` is false. With carol left out, bob keeps only his edge from
alice; with only bob and carol kept, the one edge left is bob→carol.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
without_carol = filter.Node.exclude_nodes(["carol"]).degree() > 0
assert sorted(n.name for n in g.filter(without_carol).nodes) == ["alice", "bob"]

bob_and_carol = filter.Graph.subgraph(["bob", "carol"])
assert [(e.src.name, e.dst.name) for e in g.filter(bob_and_carol).edges] == [("bob", "carol")]
```
///

## Using a property's history

`.temporal()` switches a property term from its latest value to its whole history. An aggregate
then turns the history back into one value: `.sum()`, `.avg()`, `.min()`, `.max()`, `.first()`,
`.last()`, `.len()`. Comparing the history itself gives one answer per value; `.any()` and
`.all()`, written after the comparison, ask whether any, or every, answer holds.

Two aggregates pick an update rather than reduce one: `.earliest()` and `.latest()` return the
first and last update of a history as they are. That only matters for a list-valued property,
where the aggregates above work inside each list: `.temporal().first()` is the first element of
every update, one answer per update, while `.temporal().earliest()` is the whole first list.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
total = filter.Node.property("score").temporal().sum() > 8
assert [n.name for n in g.filter(total).nodes] == ["alice"]

ever_low = (filter.Node.property("score").temporal() < 4).any()
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
| `graph.nodes[filter.Graph.window(0, 2) & expr]` | the nodes that exist in the window and match `expr` on the graph itself. On a collection a view leg is a test like any other, so `&` here is plain intersection and `expr` is not read inside the window |
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

`repr()` is the Python that builds the expression, module-qualified, so `eval` rebuilds it
after `import raphtory`. A remote graph sends the same expression, so there is no separate
server-side form to check.

/// tab | :fontawesome-brands-python: Python

```{.python continuation}
print(repr(filter.Node.window(0, 2).property("score") > 4))
```
///

!!! output

    ```
    raphtory.filter.Node.window(0, 2).property('score') > 4
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
