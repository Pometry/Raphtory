package com.raphtory.internals.graph

import com.raphtory.api.input._
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.ComponentTelemetryHandler
import com.typesafe.scalalogging.Logger
import net.openhft.hashing.LongHashFunction
import org.slf4j.LoggerFactory

private[raphtory] class GraphBuilder[T](parseFun: (Graph, T) => Unit) {
  def parse(graph: Graph, tuple: T): Unit = parseFun(graph, tuple)

  final def buildInstance(graphID: String, sourceID: Int): GraphBuilderInstance[T] =
    new GraphBuilderInstance[T](graphID, sourceID, parse)
}

private[raphtory] object GraphBuilder {

  def apply[T](parseFun: (Graph, T) => Unit): GraphBuilder[T] =
    new GraphBuilder[T](parseFun)

  def assignID(uniqueChars: String): Long =
    LongHashFunction.xx3().hashChars(uniqueChars)

}

private[raphtory] class GraphBuilderInstance[T](graphID: String, sourceID: Int, parse: (Graph, T) => Unit)
        extends Serializable
        with Graph {

  /** Logger instance for writing out log messages */
  val logger: Logger                                                  = Logger(LoggerFactory.getLogger(this.getClass))
  var index: Long                                                     = -1L
  private var partitionIDs: collection.Set[Int]                       = _
  private var writers: collection.Map[Int, EndPoint[GraphAlteration]] = _
  private var totalPartitions: Int                                    = 1
  private var sentUpdates: Long                                       = 0

  def getGraphID: String         = graphID
  def getSourceID: Int           = sourceID
  def parseTuple(tuple: T): Unit = parse(this, tuple)

  private[raphtory] def getSentUpdates: Long = sentUpdates

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
      streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]
  ): Unit = {
    writers = streamWriters
    partitionIDs = writers.keySet
    totalPartitions = writers.size
  }

  protected def updateVertexAddStats(): Unit =
    ComponentTelemetryHandler.vertexAddCounter.labels(graphID).inc()

  override def addVertex(updateTime: Long, srcId: Long, posTypeArg: Type): Unit =
    addVertex(updateTime, srcId, vertexType = posTypeArg)

  override def addVertex(
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

  override def deleteVertex(updateTime: Long, srcId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(VertexDelete(sourceID, updateTime, secondaryIndex, srcId))
    ComponentTelemetryHandler.vertexDeleteCounter.labels(graphID).inc()
  }

  protected def updateEdgeAddStats(): Unit =
    ComponentTelemetryHandler.edgeAddCounter.labels(graphID).inc()

  override def addEdge(updateTime: Long, srcId: Long, dstId: Long, posTypeArg: Type): Unit =
    addEdge(updateTime, srcId, dstId, edgeType = posTypeArg)

  override def addEdge(
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

  override def deleteEdge(updateTime: Long, srcId: Long, dstId: Long, secondaryIndex: Long = index): Unit = {
    handleGraphUpdate(EdgeDelete(sourceID, updateTime, index, srcId, dstId))
    ComponentTelemetryHandler.edgeDeleteCounter.labels(graphID).inc()
  }

  protected def handleGraphUpdate(update: GraphUpdate): Any = {
    logger.trace(s"handling $update")
    sentUpdates += 1
    val partitionForTuple = checkPartition(update.srcId)
    if (partitionIDs contains partitionForTuple) {
      writers(partitionForTuple).sendAsync(update)
      logger.trace(s"$update sent")
    }
  }

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt
}
