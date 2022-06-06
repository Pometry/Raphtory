package com.raphtory.api.input

import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** trait for creating a Graph by adding and deleting vertices and edges.
  *
  * An implementation of `GraphBuilder` needs to override `parseTuple(tuple: T)` to define parsing of input data.
  * The input data is generated using a [`Spout`](com.raphtory.components.spout.Spout) and passed to the
  * `parseTuple` method which is responsible for turning the raw data into a list of graph updates. Inside the
  * `parseTuple` implementation, use methods `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
  * for adding/deleting vertices and edges. The resulting graph updates are send to the partitions responsible for
  * handling the vertices and edges.
  *
  * Usage:
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
  val logger: Logger                                         = Logger(LoggerFactory.getLogger(this.getClass))
  private var updates: ArrayBuffer[GraphUpdate]              = ArrayBuffer()
  private var partitionIDs: mutable.Set[Int]                 = _
  private var batchWriters: mutable.Map[Int, BatchWriter[T]] = _
  private var builderID: Int                                 = _
  private var deploymentID: String                           = _
  private var batching: Boolean                              = false
  private var totalPartitions: Int                           = 1

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
  def assignID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)

  /** Parses `tuple` and fetches list of updates for the graph This is used internally to retrieve updates. */
  private[raphtory] def getUpdates(tuple: T)(failOnError: Boolean = true): List[GraphUpdate] = {
    try {
      logger.trace(s"Parsing tuple: $tuple")
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

    val toReturn = updates
    updates = ArrayBuffer()

    toReturn.toList
  }

  private[raphtory] def setBuilderMetaData(
      builderID: Int,
      deploymentID: String
  ) = {
    this.builderID = builderID
    this.deploymentID = deploymentID
  }

  private[raphtory] def setupBatchIngestion(
      IDs: mutable.Set[Int],
      writers: mutable.Map[Int, BatchWriter[T]],
      partitions: Int
  ) = {
    partitionIDs = IDs
    batchWriters = writers
    batching = true
    totalPartitions = partitions
  }

  /** Add a new vertex to the graph or update existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    */
  protected def addVertex(updateTime: Long, srcId: Long): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), None)
    handleVertexAdd(update)
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()
  }

  /** Add a new vertex to the graph or update existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    * @param properties vertex properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
    *                   available property types)
    */
  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, None)
    handleVertexAdd(update)
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()
  }

  /** Add a new vertex to the graph or update existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    * @param vertexType specify a [`Type`](com.raphtory.components.graphbuilder.Properties) for the vertex
    */
  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), Some(vertexType))
    handleVertexAdd(update)
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()
  }

  /** Add a new vertex to the graph or update existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId ID of vertex to add/update
    * @param properties vertex properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
    *                   available property types)
    * @param vertexType specify a [`Type`](com.raphtory.components.graphbuilder.Properties) for the vertex
    */
  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Type
  ): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, Some(vertexType))
    handleVertexAdd(update)
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()
  }

  /** mark vertex as deleted
    * @param updateTime time of deletion (vertex is considered as no longer present in the graph after this time)
    * @param srcId Id of vertex to delete
    */
  protected def deleteVertex(updateTime: Long, srcId: Long): Unit = {
    updates += VertexDelete(updateTime, srcId)
    ComponentTelemetryHandler.vertexDeleteCounter.labels(deploymentID).inc()
  }

  /** Add a new edge to the graph or update an existing edge
    * @param updateTime timestamp for edge update
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of destination vertex of the edge
    */
  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), None)
    handleEdgeAdd(update)
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()
  }

  /** Add a new edge to the graph or update an existing edge
    * @param updateTime timestamp for edge update
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of destination vertex of the edge
    * @param properties edge properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
    *                   available property types)
    * @param edgeType specify a [`Type`](com.raphtory.components.graphbuilder.Properties) for the edge
    */
  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, None)
    handleEdgeAdd(update)
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()
  }

  /** Add a new edge to the graph or update an existing edge
    * @param updateTime timestamp for edge update
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of destination vertex of the edge
    * @param edgeType specify a [`Type`](com.raphtory.components.graphbuilder.Properties) for the edge
    */
  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), Some(edgeType))
    handleEdgeAdd(update)
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()
  }

  /** Add a new edge to the graph or update an existing edge
    * @param updateTime timestamp for edge update
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of destination vertex of the edge
    * @param properties edge properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
    *                   available property types)
    * @param edgeType specify a [`Type`](com.raphtory.components.graphbuilder.Properties) for the edge
    */
  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Type
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, Some(edgeType))
    handleEdgeAdd(update)
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()
  }

  /** Mark edge as deleted
    * @param updateTime time of deletion (edge is considered as no longer present in the graph after this time)
    * @param srcId ID of source vertex of the edge
    * @param dstId ID of the destination vertex of the edge
    */
  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    updates += EdgeDelete(updateTime, srcId, dstId)
    ComponentTelemetryHandler.edgeDeleteCounter.labels(deploymentID).inc()
  }

  private def handleVertexAdd(update: VertexAdd) =
    if (batching) {
      val partitionForTuple = checkPartition(update.srcId)
      if (partitionIDs contains partitionForTuple)
        batchWriters(partitionForTuple).handleMessage(update)
    }
    else
      updates += update

  private def handleEdgeAdd(update: EdgeAdd) =
    if (batching) {
      val partitionForSrc = checkPartition(update.srcId)
      val partitionForDst = checkPartition(update.dstId)
      if (partitionIDs contains partitionForSrc)
        batchWriters(partitionForSrc).handleMessage(update)
      if (
              (partitionIDs contains partitionForDst) && (partitionForDst != partitionForSrc)
      ) //TODO doesn't see to currently work
        batchWriters(partitionForDst).handleMessage(
                BatchAddRemoteEdge(
                        update.updateTime,
                        update.srcId,
                        update.dstId,
                        update.properties,
                        update.eType
                )
        )
    }
    else
      updates += update

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt
}

object GraphBuilder {

  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)
}
