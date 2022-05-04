package com.raphtory.components.graphbuilder

import com.raphtory.components.partition.BatchWriter
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

import Properties._
import io.prometheus.client.Counter

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * {s}`GraphBuilder[T]`
  *   : trait for creating a Graph by adding and deleting vertices and edges.
  *
  *      {s}`T`
  *        : data type returned by the [{s}`Spout`](com.raphtory.components.spout.Spout)
  *           to be processed by the {s}`GraphBuilder`
  *
  * An implementation of {s}`GraphBuilder` needs to override {s}`parseTuple(tuple: T)` to define parsing of input data.
  * The input data is generated using a [{s}`Spout`](com.raphtory.components.spout.Spout) and passed to the
  * {s}`parseTuple` method which is responsible for turning the raw data into a list of graph updates. Inside the
  * {s}`parseTuple` implementation, use methods {s}`addVertex`/{s}`deleteVertex` and {s}`addEdge`/{s}`deleteEdge`
  * for adding/deleting vertices and edges. The resulting graph updates are send to the partitions responsible for
  * handling the vertices and edges.
  *
  * ## Methods
  *
  *    {s}`parseTuple(tuple: T): Unit`
  *      : Processes raw data message {s}`tuple` from the spout to extract source node, destination node,
  *        timestamp info, etc.
  *
  *        {s}`tuple: T`
  *          : raw input data
  *
  *        A concrete implementation of a {s}`GraphBuilder` needs to override this method to
  *        define the graph updates, calling the {s}`addVertex`/{s}`deleteVertex` and {s}`addEdge`/{s}`deleteEdge`
  *        methods documented below.
  *
  *    {s}`assignID(uniqueChars: String): Long`
  *      : Convenience method for generating unique IDs based on vertex names
  *
  *        {s}`uniqueChars: String`
  *          : Vertex name
  *
  *        Use of this method is optional. A {s}`GraphBuilder` is free to assign vertex IDs in different ways, provided
  *        that each vertex is assigned a unique ID of type {s}`Long`.
  *
  * ### Graph updates
  *
  *    {s}`addVertex(updateTime: Long, srcId: Long, properties: Properties, vertexType: Type)`
  *      : Add a new vertex to the graph or update existing vertex
  *
  *        {s}`updateTime: Long`
  *          : timestamp for vertex update
  *
  *        {s}`srcID`
  *          : ID of vertex to add/update
  *
  *        {s}`properties: Properties` (optional)
  *          : vertex properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
  *            available property types)
  *
  *        {s}`vertexType: Type` (optional)
  *          : specify a [{s}`Type`](com.raphtory.components.graphbuilder.Properties) for the vertex
  *
  *    {s}`deleteVertex(updateTime: Long, srcId: Long)`
  *      : mark vertex as deleted
  *
  *        {s}`updateTime: Long`
  *          : time of deletion (vertex is considered as no longer present in the graph after this time)
  *
  *        {s}`srcID: Long`
  *          : Id of vertex to delete
  *
  *    {s}`addEdge(updateTime: Long, srcId: Long, dstId: Long, properties: Properties, edgeType: Type)`
  *      : Add a new edge to the graph or update an existing edge
  *
  *        {s}`updateTime: Long`
  *          : timestamp for edge update
  *
  *        {s}`srcId: Long`
  *          : ID of source vertex of the edge
  *
  *        {s}`dstId: Long`
  *          : ID of destination vertex of the edge
  *
  *        {s}`properties: Properties` (optional)
  *          : edge properties for the update (see [](com.raphtory.components.graphbuilder.Properties) for the
  *            available property types)
  *
  *        {s}`edgeType: Type` (optional)
  *          : specify a [{s}`Type`](com.raphtory.components.graphbuilder.Properties) for the edge
  *
  *    {s}`deleteEdge(updateTime: Long, srcId: Long, dstId: Long)`
  *      : mark edge as deleted
  *
  *        {s}`updateTime`
  *          : time of deletion (edge is considered as no longer present in the graph after this time)
  *
  *        {s}`srcId`
  *          : ID of source vertex of the edge
  *
  *        {s}`dstId`
  *          : ID of the destination vertex of the edge
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
  *  [](com.raphtory.components.graphbuilder.Properties),
  *  [](com.raphtory.components.spout.Spout),
  *  ```
  */
trait GraphBuilder[T] extends Serializable {

  val logger: Logger                                         = Logger(LoggerFactory.getLogger(this.getClass))
  private var updates: ArrayBuffer[GraphUpdate]              = ArrayBuffer()
  private var partitionIDs: mutable.Set[Int]                 = _
  private var batchWriters: mutable.Map[Int, BatchWriter[T]] = _
  private var builderID: Int                                 = _
  private var deploymentID: String                           = _
  private var vertexAddCounter: Counter.Child                = _
  private var vertexDeleteCounter: Counter.Child             = _
  private var edgeAddCounter: Counter.Child                  = _
  private var edgeDeleteCounter: Counter.Child               = _
  private var batching: Boolean                              = false
  private var totalPartitions: Int                           = 1

  def parseTuple(tuple: T): Unit

  def assignID(uniqueChars: String): Long = LongHashFunction.xx3().hashChars(uniqueChars)

  // Parses `tuple` and fetches list of updates for the graph.
  // This is used internally to retrieve updates.
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
      deploymentID: String,
      vertexAddCounter: Counter.Child,
      vertexDeleteCounter: Counter.Child,
      edgeAddCounter: Counter.Child,
      edgeDeleteCounter: Counter.Child
  ) = {
    this.builderID = builderID
    this.deploymentID = deploymentID
    this.vertexAddCounter = vertexAddCounter
    this.vertexDeleteCounter = vertexDeleteCounter
    this.edgeAddCounter = edgeAddCounter
    this.edgeDeleteCounter = edgeDeleteCounter
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

  protected def addVertex(updateTime: Long, srcId: Long): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), None)
    handleVertexAdd(update)
    vertexAddCounter.inc()
  }

  protected def addVertex(updateTime: Long, srcId: Long, properties: Properties): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, None)
    handleVertexAdd(update)
    vertexAddCounter.inc()
  }

  protected def addVertex(updateTime: Long, srcId: Long, vertexType: Type): Unit = {
    val update = VertexAdd(updateTime, srcId, Properties(), Some(vertexType))
    handleVertexAdd(update)
    vertexAddCounter.inc()
  }

  protected def addVertex(
      updateTime: Long,
      srcId: Long,
      properties: Properties,
      vertexType: Type
  ): Unit = {
    val update = VertexAdd(updateTime, srcId, properties, Some(vertexType))
    handleVertexAdd(update)
    vertexAddCounter.inc()
  }

  protected def deleteVertex(updateTime: Long, srcId: Long): Unit = {
    updates += VertexDelete(updateTime, srcId)
    vertexDeleteCounter.inc()
  }

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), None)
    handleEdgeAdd(update)
    edgeAddCounter.inc()
  }

  protected def addEdge(
      updateTime: Long,
      srcId: Long,
      dstId: Long,
      properties: Properties
  ): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, properties, None)
    handleEdgeAdd(update)
    edgeAddCounter.inc()
  }

  protected def addEdge(updateTime: Long, srcId: Long, dstId: Long, edgeType: Type): Unit = {
    val update = EdgeAdd(updateTime, srcId, dstId, Properties(), Some(edgeType))
    handleEdgeAdd(update)
    edgeAddCounter.inc()
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
    edgeAddCounter.inc()
  }

  protected def deleteEdge(updateTime: Long, srcId: Long, dstId: Long): Unit = {
    updates += EdgeDelete(updateTime, srcId, dstId)
    edgeDeleteCounter.inc()
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
