package com.raphtory.core.components.graphbuilder

import com.raphtory.core.components.partition.BatchWriter
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

import Properties._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * {s}`GraphBuilder`
  *
  * : `GraphBuilder` trait for creating a Graph by adding and deleting vertices and edges.
  *   While using `GraphBuilder`, override {s}`parseTuple(tuple: T)` to define parsing of rows. Tuples are graph rows generated using [Spout](com.raphtory.core.components.spout.Spout).
  *   Use methods {s}`addVertex`, {s}`addEdge` for adding vertices and edge to the graph. Deletion can be performed with {s}`deleteEdge` and {s}`deleteVertex` methods.
  *
  *   `GraphBuilder` creates a list of updates {s}`GraphUpdate` representing objects signifying vertex/edge add/delete
  *   along with graph alteration information like update timestamp.
  *   These updates are retrieved and processed by the `Writer` and stored into `GraphPartition` at the time of ingesting vertices, edges and processing updates.
  *
  * ## Methods
  *
  *    {s}`parseTuple(tuple: T): Unit`
  *      : Processes messages {s}`tuple` representing a singular row from the spout to extract source node, destination node, timestamp info, etc.
  *        Creates vertices and edges with the parsed information, find example usage below.
  *
  *    {s}`addVertex(updateTime: Long, srcId: Long, properties: Properties, vertexType: Type)`
  *      : Adds {s}`VertexAdd` to the list of graph updates, signifies vertex addition for timestamp {s}`updateTime`, ID {s}`srcId`, [Properties](com.raphtory.core.components.graphbuilder.Properties) {s}`properties`, vertex type {s}`vertexType`
  *        Usage of {s}`Properties` and {s}`vertexType` is described in the example usage below.
  *
  *    {s}`deleteVertex(updateTime: Long, srcId: Long)`
  *      : Creates {s}`deleteVertex` that at a specific time {s}`updateTime`, removes the vertex ID {s}`srcId` from the graph scope. The vertex is still available in previous scopes.
  *        This object is sent as an update
  *
  *    {s}`deleteEdge(updateTime: Long, srcId: Long, dstId: Long)`
  *      : Creates {s}`deleteEdge` that at a specific time {s}`updateTime`, removes the edge between {s}`srcId` and {s}`dstId` from the graph scope. The edge is still available in previous scopes.
  *
  *    {s}`addEdge(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Type)`
  *      : Adds {s}`addEdge` to the list of graph updates, signifies edge addition for an edge between ID {s}`srcId` and {s}`dstId` for timestamp, {s}`updateTime` with Edge Type {s}`edgeType`, Properties {s}`properties`
  *
  *    {s}`assignID(uniqueChars: String): Long`
  *      : Assigns long hash for string {s}`uniqueChars`. This is an optional method provided for convenience.
  *
  * Example Usage:
  *
  * ```{code-block} scala
  *
  * class TwitterGraphBuilder() extends GraphBuilder[String] {
  *
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
  *
  * ```
  *
  * ```{seealso}
  *  [](com.raphtory.core.components.graphbuilder.Properties),
  *  [](com.raphtory.core.components.GraphBuilder),
  *  [](com.raphtory.core.components.spout.Spout),
  *  ```
  *
  */
trait GraphBuilder[T] extends Serializable {

  val logger: Logger                                         = Logger(LoggerFactory.getLogger(this.getClass))
  private var updates: ArrayBuffer[GraphUpdate]              = ArrayBuffer()
  private var partitionIDs: mutable.Set[Int]                 = _
  private var batchWriters: mutable.Map[Int, BatchWriter[T]] = _
  private var batching: Boolean                              = false
  private var totalPartitions: Int                           = 1

  def parseTuple(tuple: T): Unit

  def assignID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)

  // Parses `tuple` and fetches list of updates for the graph.
  // This is used internally to retrieve updates.
  private[core] def getUpdates(tuple: T)(failOnError: Boolean = true): List[GraphUpdate] = {
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

  private[core] def setupBatchIngestion(
      IDs: mutable.Set[Int],
      writers: mutable.Map[Int, BatchWriter[T]],
      partitions: Int
  ) = {
    partitionIDs = IDs
    batchWriters = writers
    batching = true
    totalPartitions = partitions
  }

  protected def addVertex(updateTime: Long, srcId: Long): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), None)
    handleVertexAdd(update)
  }

  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, None)
    handleVertexAdd(update)
  }

  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), Some(vertexType))
    handleVertexAdd(update)
  }

  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Type
  ): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, Some(vertexType))
    handleVertexAdd(update)
  }

  protected def deleteVertex(updateTime: Long, srcId: Long): Unit =
    updates += VertexDelete(updateTime, srcId)

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), None)
    handleEdgeAdd(update)
  }

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, None)
    handleEdgeAdd(update)

  }

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), Some(edgeType))
    handleEdgeAdd(update)

  }

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties,
      edgeType: Type
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, Some(edgeType))
    handleEdgeAdd(update)

  }

  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long): Unit =
    updates += EdgeDelete(updateTime, srcId, dstId)

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
