package com.raphtory.api.input

import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.EdgeDelete
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import com.raphtory.internals.graph.GraphAlteration.VertexDelete
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

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
  * @see [[Properties]] [[Spout]]
  */
trait Graph {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def totalPartitions: Int
  protected def handleGraphUpdate(update: GraphUpdate): Unit
  protected def sourceID: Int
  def index: Long
  protected def graphID: String

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param posTypeArg specify a [[Type Type]] for the vertex
    */
  def addVertex(updateTime: Long, srcId: Long, posTypeArg: Type): Unit =
    addVertex(updateTime, srcId, vertexType = posTypeArg)

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
      secondaryIndex: Long = index
  ): Unit = {
    val update = VertexAdd(sourceID, updateTime, secondaryIndex, srcId, properties, vertexType.toOption)
    logger.trace(s"Created update $update")
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  /** Marks a vertex as deleted
    * @param updateTime time of deletion (a vertex is considered as no longer present in the graph after this time)
    * @param srcId Id of vertex to delete
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(VertexDelete(sourceID, updateTime, secondaryIndex, srcId))
    ComponentTelemetryHandler.vertexDeleteCounter.labels(graphID).inc()
  }

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
      secondaryIndex: Long = index
  ): Unit = {
    val update = EdgeAdd(sourceID, updateTime, secondaryIndex, srcId, dstId, properties, edgeType.toOption)
    handleGraphUpdate(update)
    updateEdgeAddStats()
  }

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param posTypeArg   specify a [[Type Type]] for the edge
    */
  def addEdge(updateTime: Long, srcId: Long, dstId: Long, posTypeArg: Type): Unit =
    addEdge(updateTime, srcId, dstId, edgeType = posTypeArg)

  /** Mark edge as deleted
    * @param updateTime time of deletion (the edge is considered as no longer present in the graph after this time)
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of the destination vertex of the edge
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(EdgeDelete(sourceID, updateTime, index, srcId, dstId))
    ComponentTelemetryHandler.edgeDeleteCounter.labels(graphID).inc()
  }

  /** Convenience method for generating unique IDs based on vertex names
    *
    * Use of this method is optional. A `GraphBuilder` is free to assign vertex IDs in different ways, provided
    * that each vertex is assigned a unique ID of type `Long`.
    *
    * @param uniqueChars Vertex name
    */
  def assignID(uniqueChars: String): Long =
    Graph.assignID(uniqueChars)

  private[raphtory] def getPartitionForId(id: Long): Int =
    (id.abs % totalPartitions).toInt

  protected def updateVertexAddStats(): Unit =
    ComponentTelemetryHandler.vertexAddCounter.labels(graphID).inc()

  protected def updateEdgeAddStats(): Unit =
    ComponentTelemetryHandler.edgeAddCounter.labels(graphID).inc()

}

object Graph {

  /** Convenience method for generating unique IDs based on vertex names
    *
    * Use of this method is optional. A `GraphBuilder` is free to assign vertex IDs in different ways, provided
    * that each vertex is assigned a unique ID of type `Long`.
    *
    * @param uniqueChars Vertex name
    */
  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)

}
