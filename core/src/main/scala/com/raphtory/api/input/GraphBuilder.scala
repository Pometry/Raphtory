package com.raphtory.api.input

import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
trait GraphBuilder[T] extends Serializable {

  /** Logger instance for writing out log messages */
  val logger: Logger                                              = Logger(LoggerFactory.getLogger(this.getClass))
  private var index: Long                                         = -1L
  private var partitionIDs: collection.Set[Int]                   = _
  private var writers: collection.Map[Int, EndPoint[GraphUpdate]] = _
  private var builderID: Int                                      = _
  private var deploymentID: String                                = _
  private var batching: Boolean                                   = false
  private var totalPartitions: Int                                = 1

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    *  A concrete implementation of a `GraphBuilder` needs to override this method to
    *  define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    *  methods documented below.
    *
    *  @param tuple raw input data
    */
  def parseTuple(tuple: T): Unit

  /** Convenience method for generating unique IDs based on vertex names
    *
    * Use of this method is optional. A `GraphBuilder` is free to assign vertex IDs in different ways, provided
    * that each vertex is assigned a unique ID of type `Long`.
    *
    * @param uniqueChars Vertex name
    */
  def assignID(uniqueChars: String): Long = GraphBuilder.assignID(uniqueChars)

  /** Parses `tuple` and fetches list of updates for the graph This is used internally to retrieve updates. */
  private[raphtory] def sendUpdates(tuple: T, tupleIndex: Long)(failOnError: Boolean = true): Unit =
    try {
      logger.trace(s"Parsing tuple: $tuple with index $tupleIndex")
      index = tupleIndex
      parseTuple(tuple)
    }
    catch {
      case e: Exception =>
        if (failOnError)
          throw e
        else {
          logger.warn(s"Failed to parse tuple.", e.getMessage)
          e.printStackTrace()
        }
    }

  private[raphtory] def setBuilderMetaData(
      builderID: Int,
      deploymentID: String
  ): Unit = {
    this.builderID = builderID
    this.deploymentID = deploymentID
  }

  private[raphtory] def setupBatchIngestion(
      IDs: mutable.Set[Int],
      batchWriters: collection.Map[Int, BatchWriter[T]],
      partitions: Int
  ): Unit = {
    partitionIDs = IDs
    writers = batchWriters
    batching = true
    totalPartitions = partitions
  }

  private[raphtory] def setupStreamIngestion(
      streamWriters: collection.Map[Int, EndPoint[GraphUpdate]]
  ): Unit = {
    writers = streamWriters
    partitionIDs = writers.keySet
    batching = false
    totalPartitions = writers.size
  }

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    */
  protected def addVertex(updateTime: Long, srcId: Long): Unit = {
    val update = VertexAdd(updateTime, index, srcId, Properties(), None)
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  protected def updateVertexAddStats(): Unit =
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    * @param properties vertex properties for the update (see [[com.raphtory.api.input.Properties Properties]] for the
    *                   available property types)
    */
  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit = {
    val update = VertexAdd(updateTime, index, srcId, properties, None)
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param vertexType specify a [[Type Type]] for the vertex
    */
  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit = {
    val update = VertexAdd(updateTime, index, srcId, Properties(), Some(vertexType))
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param properties Optionally specify vertex properties for the update (see [[com.raphtory.api.input.Properties Properties]] for the
    *                   available property types)
    * @param vertexType Optionally specify a [[Type Type]] for the vertex
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties = Properties(),
      vertexType: MaybeType = NoType,
      secondaryIndex: Long = index
  ): Unit = {
    val update = VertexAdd(updateTime, secondaryIndex, srcId, properties, vertexType.toOption)
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  /** Marks a vertex as deleted
    * @param updateTime time of deletion (a vertex is considered as no longer present in the graph after this time)
    * @param srcId Id of vertex to delete
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  protected def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(VertexDelete(updateTime, secondaryIndex, srcId))
    ComponentTelemetryHandler.vertexDeleteCounter.labels(deploymentID).inc()
  }

  /** Adds a new edge to the graph or updates an existing edge
    * @param updateTime timestamp for edge update
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of destination vertex of the edge
    */
  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    val update = EdgeAdd(updateTime, index, srcId, dstId, Properties(), None)
    handleEdgeAdd(update)
    updateEdgeAddStats()
  }

  protected def updateEdgeAddStats(): Unit =
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()

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
  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): Unit = {
    val update = EdgeAdd(updateTime, index, srcId, dstId, properties, None)
    handleEdgeAdd(update)
    updateEdgeAddStats()
  }

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param edgeType   specify a [[Type Type]] for the edge
    */
  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit = {
    val update = EdgeAdd(updateTime, index, srcId, dstId, Properties(), Some(edgeType))
    handleEdgeAdd(update)
    updateEdgeAddStats()
  }

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param properties edge properties for the update (see [[com.raphtory.api.input.Properties Properties]] for the
    *                   available property types)
    * @param edgeType   specify a [[Type Type]] for the edge
    */
  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties = Properties(),
      edgeType: MaybeType = NoType,
      secondaryIndex: Long = index
  ): Unit = {
    val update = EdgeAdd(updateTime, secondaryIndex, srcId, dstId, properties, edgeType.toOption)
    handleEdgeAdd(update)
    updateEdgeAddStats()
  }

  /** Mark edge as deleted
    * @param updateTime time of deletion (the edge is considered as no longer present in the graph after this time)
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of the destination vertex of the edge
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(EdgeDelete(updateTime, index, srcId, dstId))
    ComponentTelemetryHandler.edgeDeleteCounter.labels(deploymentID).inc()
  }

  protected def handleGraphUpdate(update: GraphUpdate): Any = {
    val partitionForTuple = checkPartition(update.srcId)
    if (partitionIDs contains partitionForTuple)
      writers(partitionForTuple).sendAsync(update)
  }

  protected def handleEdgeAdd(update: EdgeAdd): Any = {
    val partitionForSrc = checkPartition(update.srcId)
    if (partitionIDs contains partitionForSrc)
      writers(partitionForSrc).sendAsync(update)
    if (batching) {
      val partitionForDst = checkPartition(update.dstId)
      if (
              (partitionIDs contains partitionForDst) && (partitionForDst != partitionForSrc)
      ) //TODO doesn't see to currently work
        writers(partitionForDst).sendAsync(
                BatchAddRemoteEdge(
                        update.updateTime,
                        index,
                        update.srcId,
                        update.dstId,
                        update.properties,
                        update.eType
                )
        )
    }
  }

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt
}

object GraphBuilder {

  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)
}
