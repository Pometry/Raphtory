package com.raphtory.api.input

/** trait for creating a Graph by adding and deleting vertices and edges.
  *
  * An implementation of `GraphBuilder` needs to override `parseTuple(tuple: T)` to define parsing of input data.
  * The input data is generated using a [[com.raphtory.api.input.Spout Spout]] and passed to the
  * `parseTuple` method which is responsible for turning the raw data into a list of graph updates. Inside the
  * `parseTuple` implementation, use methods `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
  * for adding/deleting vertices and edges. The resulting graph updates are send to the partitions responsible for
  * handling the vertices and edges.
  *
  * @example
  * {{{
  * class TwitterGraphBuilder() extends GraphBuilder[String] {
  *   override def parseTuple(fileLine: String): Unit = {
  *     val sourceNode = fileLine(0)
  *     val srcID      = sourceNode.toLong
  *     val targetNode = fileLine(1)
  *     val tarID      = targetNode.toLong
  *     val timeStamp  = fileLine(2).toLong
  *
  *     addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("User"))
  *     addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("User"))
  *     addEdge(timeStamp, srcID, tarID, Type("Follows"))
  *   }
  * }
  * }}}
  *
  * @see [[Properties]] [[Spout]]
  */
trait Graph {

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param posTypeArg specify a [[Type Type]] for the vertex
    */
  def addVertex(updateTime: Long, srcId: Long, posTypeArg: Type): Unit

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param properties Optionally specify vertex properties for the update (see [[com.raphtory.api.input.Properties Properties]] for the
    *                   available property types)
    * @param vertexType Optionally specify a [[Type Type]] for the vertex
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties = Properties(),
      vertexType: MaybeType = NoType,
      secondaryIndex: Long = 1 //this is always overwritten its just to make the API happy
  ): Unit

  /** Marks a vertex as deleted
    * @param updateTime time of deletion (a vertex is considered as no longer present in the graph after this time)
    * @param srcId Id of vertex to delete
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = 1): Unit

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param posTypeArg   specify a [[Type Type]] for the edge
    */
  def addEdge(updateTime: Long, srcId: Long, dstId: Long, posTypeArg: Type): Unit

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param properties edge properties for the update (see [[com.raphtory.api.input.Properties Properties]] for the
    *                   available property types)
    * @param edgeType   specify a [[Type Type]] for the edge
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties = Properties(),
      edgeType: MaybeType = NoType,
      secondaryIndex: Long = 1 //this is always overwritten its just to make the API happy
  ): Unit

  /** Mark edge as deleted
    * @param updateTime time of deletion (the edge is considered as no longer present in the graph after this time)
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of the destination vertex of the edge
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = 1): Unit
}
