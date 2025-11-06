# Key concepts

To use Raphtory effectively it is useful to understand the specific ways that the tool represents graph objects and the tools you have to manipulate them.

## Graphs

In the most general sense, a graph is set of entities (nodes or vertices) where you can define relationships (links or edges) between pairs of entities. The relationships between a pairs of entities may be directed or undirected and the overall graph will have different properties as a result.

In Raphtory we adopt the naming convention of nodes and edges. All Raphtory graphs are directional and edges have a source and destination, although loops are allowed so the source and destination node may be the same.

Additionally, Raphtory graphs are designed to capture temporal relationships. You can view temporal data as either a stream of discrete events or a persistent state over a duration of time and Raphtory allows you to adopt either perspective.

To represent your data as a stream you can use a [Graph][raphtory.Graph] object and to adopt the extended events representation you can use a [PersistentGraph][raphtory.PersistentGraph] object. Raphtory allows you to easily switch between these representations by calling `event_graph()` or `persistent_graph()`.

### The `Graph` object

When beginning a new project you will need to [create a new `Graph` object](../ingestion/1_intro.md). You can start by start adding nodes and edges to the graph from scratch or by importing existing data in standard formats.

As you get new data you can make global updates, like adding new nodes, using the `Graph` object and make global queries using a [GraphView][raphtory.GraphView]. You can also make local updates to the properties and metadata of individual nodes and edges.

## Nodes

The [Node][raphtory.Node] object in Raphtory typically represents some entity in your data. You create nodes using the [.add_node()][raphtory.Graph.add_node] function on your `Graph` object, subsequent updates can be made using a [MutableNode][raphtory.MutableNode] that you get by calling `my_graph.node(node_id)`. Additionally, calling `.add_node()` repeatedly will not overwrite the original object, instead additional properties and events are added to the existing object.

Queries are performed using the Node object or an appropriate view. To make queries more convenient Raphtory provides the [Nodes][raphtory.Nodes] iterable that allows you to make queries over all the nodes in the current view. Typically, queries on an individual Node will return a result directly, while queries over the `Nodes` iterable will often return a view or NodeState.

NodeStates are tabular representations of a collection across all nodes in the current view. You can easily transform a NodeState into a dataframe or other tabular format to integrate into your existing workflow.

!!! Info
    The Nodes and Edges iterables have a `.collect()` function that returns list of all the your objects. This operates on the underlying Rust library and is much faster than creating the list manually in Python.

## Edges

An [Edge][raphtory.Edge] object represents relationships between nodes. However, in Raphtory there is only ever one edge between any pair of nodes. This unified edge object combines interactions across all times and all relationships.

Some graph tools create multiple edges for each relationship or time dependent interaction between nodes, you can replicate this view in Raphtory by calling `.explode()` on the unified edge object.

To represent multiple relationships you can use the properties and metadata of an edge or create edges on specific layers which are agreagated into the unified edge object.

Similarly to nodes, you must create edges using [.add_edge()][raphtory.Graph.add_edge] and can make changes using a [MutableEdge][raphtory.MutableEdge] object or by calling `.add_edge()` repeatedly. There is also an [Edges][raphtory.Edges] iterable that allows you to make queries over all the edges in the current view.

An Edge object contains the combined information for that edge across all points in time. Often it is more useful to look at the changes across time. To do this Raphtory provides the option to [explode][raphtory.Edge.explode] an edge, which returns an edge object for each update within the original edge.

## Layers

To further separate different types of relationships between the same types of entities, you can create separate layers in the same graph. The nodes of a graph exist across all layers but edges can be assigned a specific layer. In the [introductionary example](1_intro.md) we use edges on separate layers to distinguish between different behaviours of a troop of baboons.

Queries will show edges across all layers by default and edges that exist on multiple layers are combined into a single edge with the combined properties and history. You can apply an appropriate view to work with a specific layer or layers.

## Properties and metadata

Graphs, nodes, and edges can all have properties and metadata. In Raphtory, properties are metrics that can vary in time and each update caries a timestamp, while metadata are metrics that are constant across time (you can edit metadata but they have no history). This is system is highly flexible and the main way that you add complex data to the graph structure.

When making a query, entities return [Properties][raphtory.Properties] or [Metadata][raphtory.Metadata] objects that contain all the data within the current view. You can use these objects to fetch and manipulate any individual entries of interest.

## Views and filters

Views are objects in Raphtory that provide a read-only subset of your data, this means you can have many views without expensive data duplication. A [GraphView][raphtory.GraphView] provides a view of the graph reduced to a specific subset by a filter or time function. Similarly, many other functions provide filtered views of edges or nodes.

Views can be an instance of a `Node` or `Nodes`, `Edge` or `Edges`, `GraphView` or a dedicated class from the [node_state][raphtory.node_state] module.

For example, calling `edge_foo.layer("named_layer")` returns an `Edge` object that acts as a view of the edge restricted to the `named_layer` while calling `nodes_bar.latest_time` returns a [LatestTimeView][raphtory.node_state.LatestTimeView] object.

## History

Updates to entities in Raphtory are recorded in a history of events which can be accessed through a dedicated [History][raphtory.History] object. Individual events are represented by a [EventTime][raphtory.EventTime] which combines a primary timestamp and a secondary index used to ensure fixed ordering when events occur at the same time.

You can transform a `EventTime` into a UTC datetime representation or Unix timestamp in milliseconds as needed. This will always be based on the primary timestamp, the secondary is only used for ordering and will be automatically generated by Raphtory unless specified explicitly by the user. This makes it easy to integrate raphtory events into your existing tooling.

When looking at an overall history the `History` object provides a lot of flexibility including iterables and views to work with datetimes and Unix times directly and methods to combine and compare histories. This allows you to analyse both individual entities and groups, for example, by combining the histories of nodes that share a common community.
