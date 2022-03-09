`com.raphtory.core.components.graphbuilder.GraphBuilder`
(com.raphtory.core.components.graphbuilder.GraphBuilder)=
# GraphBuilder

{s}`GraphBuilder[T]`
  : trait for creating a Graph by adding and deleting vertices and edges.

     {s}`T`
       : data type returned by the [{s}`Spout`](com.raphtory.core.components.spout.Spout)
          to be processed by the {s}`GraphBuilder`

An implementation of {s}`GraphBuilder` needs to override {s}`parseTuple(tuple: T)` to define parsing of input data.
The input data is generated using a [{s}`Spout`](com.raphtory.core.components.spout.Spout) and passed to the
{s}`parseTuple` method which is responsible for turning the raw data into a list of graph updates. Inside the
{s}`parseTuple` implementation, use methods {s}`addVertex`/{s}`deleteVertex` and {s}`addEdge`/{s}`deleteEdge`
for adding/deleting vertices and edges. The resulting graph updates are send to the partitions responsible for
handling the vertices and edges.

## Methods

   {s}`parseTuple(tuple: T): Unit`
     : Processes raw data message {s}`tuple` from the spout to extract source node, destination node,
       timestamp info, etc.

       {s}`tuple: T`
         : raw input data

       A concrete implementation of a {s}`GraphBuilder` needs to override this method to
       define the graph updates, calling the {s}`addVertex`/{s}`deleteVertex` and {s}`addEdge`/{s}`deleteEdge`
       methods documented below.

   {s}`assignID(uniqueChars: String): Long`
     : Convenience method for generating unique IDs based on vertex names

       {s}`uniqueChars: String`
         : Vertex name

       Use of this method is optional. A {s}`GraphBuilder` is free to assign vertex IDs in different ways, provided
       that each vertex is assigned a unique ID of type {s}`Long`.

### Graph updates

   {s}`addVertex(updateTime: Long, srcId: Long, properties: Properties, vertexType: Type)`
     : Add a new vertex to the graph or update existing vertex

       {s}`updateTime: Long`
         : timestamp for vertex update

       {s}`srcID`
         : ID of vertex to add/update

       {s}`properties: Properties` (optional)
         : vertex properties for the update (see [](com.raphtory.core.components.graphbuilder.Properties) for the
           available property types)

       {s}`vertexType: Type` (optional)
         : specify a [{s}`Type`](com.raphtory.core.components.graphbuilder.Properties) for the vertex

   {s}`deleteVertex(updateTime: Long, srcId: Long)`
     : mark vertex as deleted

       {s}`updateTime: Long`
         : time of deletion (vertex is considered as no longer present in the graph after this time)

       {s}`srcID: Long`
         : Id of vertex to delete

   {s}`addEdge(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Type)`
     : Add a new edge to the graph or update an existing edge

       {s}`updateTime: Long`
         : timestamp for edge update

       {s}`srcId: Long`
         : ID of source vertex of the edge

       {s}`dstId: Long`
         : ID of destination vertex of the edge

       {s}`properties: Properties` (optional)
         : edge properties for the update (see [](com.raphtory.core.components.graphbuilder.Properties) for the
           available property types)

       {s}`edgeType: Type` (optional)
         : specify a [{s}`Type`](com.raphtory.core.components.graphbuilder.Properties) for the edge

   {s}`deleteEdge(updateTime: Long, srcId: Long, dstId: Long)`
     : mark edge as deleted

       {s}`updateTime`
         : time of deletion (edge is considered as no longer present in the graph after this time)

       {s}`srcId`
         : ID of source vertex of the edge

       {s}`dstId`
         : ID of the destination vertex of the edge

Example Usage:

```{code-block} scala

class TwitterGraphBuilder() extends GraphBuilder[String] {

  override def parseTuple(fileLine: String): Unit = {
    val sourceNode = fileLine(0)
    val srcID      = sourceNode.toLong
    val targetNode = fileLine(1)
    val tarID      = targetNode.toLong
    val timeStamp  = fileLine(2).toLong

    addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
    addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
    addEdge(timeStamp, srcID, tarID, Type("Follows"))
  }
}

```

```{seealso}
 [](com.raphtory.core.components.graphbuilder.Properties),
 [](com.raphtory.core.components.spout.Spout),
 ```