package com.raphtory.api.input

import com.raphtory.internals.communication.EndPoint
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
trait GraphBuilder[T] {

  /** Processes raw data message `tuple` from the spout to extract source node, destination node,
    * timestamp info, etc.
    *
    *  A concrete implementation of a `GraphBuilder` needs to override this method to
    *  define the graph updates, calling the `addVertex`/`deleteVertex` and `addEdge`/`deleteEdge`
    *  methods documented below.
    *
    *  @param tuple raw input data
    */
  def parse(graph: Graph, tuple: T): Unit

  /** Convenience method for generating unique IDs based on vertex names
    *
    * Use of this method is optional. A `GraphBuilder` is free to assign vertex IDs in different ways, provided
    * that each vertex is assigned a unique ID of type `Long`.
    *
    * @param uniqueChars Vertex name
    */
  final def assignID(uniqueChars: String): Long = GraphBuilder.assignID(uniqueChars)

  final def buildInstance(deploymentID: String): GraphBuilderInstance[T] =
    new GraphBuilderInstance[T] {
      override def getDeploymentID: String    = deploymentID
      override def parseTuple(tuple: T): Unit = parse(this, tuple)
    }
}

trait GraphBuilderInstance[T] extends Serializable with Graph {

  /** Logger instance for writing out log messages */
  val logger: Logger                                              = Logger(LoggerFactory.getLogger(this.getClass))
  var index: Long                                                 = -1L
  private val deploymentID                                        = getDeploymentID
  private var partitionIDs: collection.Set[Int]                   = _
  private var writers: collection.Map[Int, EndPoint[GraphUpdate]] = _
  private var totalPartitions: Int                                = 1
  private val batching: Boolean                                   = false

  def getDeploymentID: String
  def parseTuple(tuple: T): Unit

  /** Parses `tuple` and fetches list of updates for the graph This is used internally to retrieve updates. */
  private[raphtory] def sendUpdates(tuple: T, tupleIndex: Long)(failOnError: Boolean = true): Unit =
    try {
      logger.trace(s"Parsing tuple: $tuple with index $tupleIndex")
      index = tupleIndex
      parseTuple(tuple)
    }
    catch {
      case e: Exception =>
        if (failOnError) {
          e.printStackTrace()
          throw e
        }
        else {
          logger.warn(s"Failed to parse tuple.", e.getMessage)
          e.printStackTrace()
        }
    }

  private[raphtory] def setupStreamIngestion(
      streamWriters: collection.Map[Int, EndPoint[GraphUpdate]]
  ): Unit = {
    writers = streamWriters
    partitionIDs = writers.keySet
    totalPartitions = writers.size
  }

  protected def updateVertexAddStats(): Unit =
    ComponentTelemetryHandler.vertexAddCounter.labels(deploymentID).inc()

  /** Adds a new vertex to the graph or updates an existing vertex
    *
    * @param updateTime timestamp for vertex update
    * @param srcId      ID of vertex to add/update
    * @param posTypeArg specify a [[Type Type]] for the vertex
    */
  override def addVertex(updateTime: Long, srcId: Long, posTypeArg: Type): Unit =
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
  override def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties = Properties(),
      vertexType: MaybeType = NoType,
      secondaryIndex: Long = index
  ): Unit = {
    val update = VertexAdd(updateTime, secondaryIndex, srcId, properties, vertexType.toOption)
    logger.trace(s"Created update $update")
    handleGraphUpdate(update)
    updateVertexAddStats()
  }

  /** Marks a vertex as deleted
    * @param updateTime time of deletion (a vertex is considered as no longer present in the graph after this time)
    * @param srcId Id of vertex to delete
    * @param secondaryIndex Optionally specify a secondary index that is used to determine the order of updates with the same `updateTime`
    */
  override def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(VertexDelete(updateTime, secondaryIndex, srcId))
    ComponentTelemetryHandler.vertexDeleteCounter.labels(deploymentID).inc()
  }

  protected def updateEdgeAddStats(): Unit =
    ComponentTelemetryHandler.edgeAddCounter.labels(deploymentID).inc()

  /** Adds a new edge to the graph or updates an existing edge
    *
    * @param updateTime timestamp for edge update
    * @param srcId      ID of source vertex of the edge
    * @param dstId      ID of destination vertex of the edge
    * @param posTypeArg   specify a [[Type Type]] for the edge
    */
  override def addEdge(updateTime: Long, srcId: Long, dstId: Long, posTypeArg: Type): Unit =
    addEdge(updateTime, srcId, dstId, edgeType = posTypeArg)

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
  override def addEdge(
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
  override def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(EdgeDelete(updateTime, index, srcId, dstId))
    ComponentTelemetryHandler.edgeDeleteCounter.labels(deploymentID).inc()
  }

  protected def handleGraphUpdate(update: GraphUpdate): Any = {
    logger.trace(s"handling $update")
    val partitionForTuple = checkPartition(update.srcId)
    if (partitionIDs contains partitionForTuple) {
      writers(partitionForTuple).sendAsync(update)
      logger.trace(s"$update sent")
    }
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

trait Graph {

  def addVertex(updateTime: Long, srcId: Long, posTypeArg: Type): Unit

  def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties = Properties(),
      vertexType: MaybeType = NoType,
      secondaryIndex: Long = 1 //this is always overwritten its just to make the API happy
  ): Unit
  def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = 1): Unit
  def addEdge(updateTime: Long, srcId: Long, dstId: Long, posTypeArg: Type): Unit

  def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties = Properties(),
      edgeType: MaybeType = NoType,
      secondaryIndex: Long = 1 //this is always overwritten its just to make the API happy
  ): Unit
  def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = 1): Unit
}
