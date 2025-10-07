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

The [Node][raphtory.Node] object in Raphtory typically represents some entity in your data. You must create nodes using the [.add_node()][raphtory.Graph.add_node] function on your Graph object and any subsequent updates must be made using a [MutableNode][raphtory.MutableNode] that you can get by calling `my_graph.node(node_id)`. Any queries are performed using the Node object or an appropriate view.

To make queries more convenient Raphtory provides the [Nodes][raphtory.Nodes] iterable that allows you to make queries over all the nodes in the current view. Typically, queries on an individual Node will return a result directly, while queries over the Nodes iterable will return a view.

!!! Info
    The Nodes and Edges iterables have a `.collect()` function that returns list of all the your objects. This is operates on the underlying Rust library and is much faster than creating the list manually in Python.

## Edges

The [Edge][raphtory.Edge] object represents relationships between nodes. However, in Raphtory there can only be one edge between any pair of nodes so to represent multiple relationships you must use properties and metadata.

Similarly to nodes, you must create edges using [.add_edge()][raphtory.Graph.add_edge] and make changes using a [MutableEdge][raphtory.MutableEdge] object. There is also an [Edges][raphtory.Edges] iterable that allows you to make queries over all the nodes in the current view.

An Edge object contains the combined information for that edge across all points in time. Often it is more useful to look at the changes across time. To do this Raphtory provides the option to [explode][raphtory.Edge.explode] an edge, which returns an edge object for each update within the original edge.

## Properties and metadata

Graphs, nodes, and edges can all have properties and metadata. In Raphtory, properties are metrics that can vary in time and each update caries a timestamp, while metadata are metrics that are immutable (you can edit metadata but they have no history). This is system is highly flexible and the main way that you add complex data to the graph structure.

When making a query, entities return [Properties][raphtory.Properties] or [Metadata][raphtory.Metadata] objects that contain all the data within the current view. You can use these objects to fetch and manipulate any individual entries of interest.

## Layers

To further separate different types of relationships between the same types of entities, you can create separate layers in the same graph. The nodes of a graph exist across all layers but edges can be assigned a specific layer. In the [introductionary example](1_intro.md) we use edges on separate layers to distinguish between different behaviours of the baboons.

Queries will show edges across all layers by default and edges that exist on multiple layers are combined into a single edge with the combined properties and history. You can apply an appropriate view to work with a specific layer or layers.

## Views and filters

Views are objects in Raphtory that provide read-only of a subsets of your data, this means you can have many views without expensive data duplication. A [GraphView][raphtory.GraphView] provides a view of the graph reduced to a specific subset by a filter or time function. Similarly, many other functions provide filtered views of edges or nodes.

Views can be an instance of a `Node`, `Edge`, `GraphView` or a dedicated class from the [node_state][raphtory.node_state] module.

For example, calling `edge_foo.layer("named_layer")` returns an `Edge` object that acts as a view of the edge restricted to the `named_layer` while calling `node_bar.latest_time` returns a [LatestTimeView][raphtory.node_state.LatestTimeView] object.

## History

Updates to entities in Raphtory are recorded in a history of events which can be accessed through a dedicated [History][raphtory.History] object. Individual events are represented by a [EventTime][raphtory.EventTime] which combines a primary timestamp and a secondary index used to ensure fixed ordering when events occur at the same time.

You can transform a `EventTime` into a UTC datetime representation or Unix timestamp in milliseconds as needed. This will always be based on the primary timestamp, the secondary is only used for ordering and will be automatically generated by Raphtory unless specified explicitly by the user. This makes it easy to integrate raphtory events into your existing tooling.

When looking at an overall history the `History` object provides a lot of flexibility including iterables and views to work with datetimes and Unix times directly and methods to combine and compare histories. This allows you to analyse both individual entities and groups, for example, by combining the histories of nodes that share a common community.
