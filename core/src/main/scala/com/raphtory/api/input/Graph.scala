package com.raphtory.api.input

import com.raphtory.internals.communication.SchemaProviderInstances._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.internals.time.DateTimeParser
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

/** trait for creating a Graph by adding and deleting vertices and edges.
  *
  * An implementation of `GraphBuilder` needs to override `parseTuple(tuple: T)` to define parsing of input data.
  * The input data is generated using a [[com.raphtory.api.input.Spout Spout]] and passed to the
  * `parseTuple` method which is responsible for turning the raw data into a list of graph updates. Inside the
  * `parseTuple` implementation, use methods `addVertex` and `addEdge`
  * for adding vertices and edges. The resulting graph updates are send to the partitions responsible for
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

  def totalPartitions: Int
  protected def handleGraphUpdate(update: GraphUpdate): Unit
  protected def sourceID: Int
  def index: Long
  def graphID: String

  private def vertexAddCounter = TelemetryReporter.vertexAddCounter.labels(s"$sourceID", graphID)

  private def edgeAddCounter = TelemetryReporter.edgeAddCounter.labels(s"$sourceID", graphID)

  def getNEdgesAdded : Long = edgeAddCounter.get().toLong
  def getNVerticesAdded : Long = vertexAddCounter.get().toLong

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
    handleGraphUpdate(update)
    vertexAddCounter.inc()
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
    edgeAddCounter.inc()
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

  /** Convenience method for converting dates into epochs to be used internally by Raphtory. This is the same function
    * used by the `add vertex` and `add edge` functions, so can be called explicitly in a graph builder
    * to reduce the number of conversions when performing multiple updates per tuple.
    *
    * @param dateTime The date string you wish to convert
    * @param format The format of the date string. The default being "yyyy-MM-dd[ HH:mm:ss[.SSS]]."
    * .
    */
  def parseDatetime(dateTime: String, format: String = "yyyy-MM-dd[ HH:mm:ss[.SSS]]"): Long =
    DateTimeParser(format).parse(dateTime)
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
