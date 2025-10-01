# Key concepts

To use Raphtory effectively it is useful to understand the specific ways that the tool represents graph objects and the tools you have to manipulate them.

## Graphs

In the most general sense, a graph is set of objects (nodes or vertices) where you can define relationships (links or edges) between pairs of objects. The relationships between a pairs of objects may be directed or undirected and the overall graph will have different properties as a result.

In Raphtory we adopt the naming convention of nodes and edges. All Raphtory graphs are directional and edges have a source and destination, although loops are allowed so the source and destination node may be the same.

Additionally, Raphtory graphs are designed to capture temporal relationships which can be categorised as either a stream of discrete events or a persistent state over a duration of time. To represent these two cases Raphtory provides the [Graph][raphtory.Graph] and [PersistentGraph][raphtory.PersistentGraph] objects.

### The `Graph` object

When beginning a new project you will need to select which type of graph you need based on the data you want to investigate then [create a new `Graph` object](../ingestion/1_intro.md). You will typically begin by adding nodes and edges to the graph from scratch or by import existing data in standard formats.

As you get new data you can make global updates, like adding new nodes, using the `Graph` object and make global queries using a [GraphView][raphtory.GraphView].

## Nodes

The [Node][raphtory.Node] object in Raphtory typically represents some entity in your data. You must create nodes using the [add_node()][raphtory.Graph.add_node] function on your Graph object and any subsequent updates must be made using a [MutableNode][raphtory.MutableNode] that you can get by calling `my_graph.node(node_id)`. Any queries are performed using the Node object or an appropriate view.

To make queries more convenient Raphtory provides the [Nodes][raphtory.Nodes] iterable that allows you to make queries over all the nodes in the current view. Typically, queries on an individual Node will return a result directly, while queries over the Nodes iterable will return a view.

## Edges

However, there can only be one edge between any pair of nodes

## Properties and metadata

## Layers

## Views

## History
