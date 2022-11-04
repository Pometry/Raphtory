package com.raphtory.internals.graph

import com.raphtory.api.input._
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.TelemetryReporter

private[raphtory] class GraphBuilderInstance[T](graphId: String, sourceId: Int, parse: GraphBuilder[T])
        extends Serializable
        with Graph {

  override protected def sourceID: Int = sourceId

  override protected def graphID: String = graphId

  /** Logger instance for writing out log messages */
  var highestTimeSeen: Long                                           = Long.MinValue
  var internalIndex: Long                                             = -1L
  override def index: Long                                            = internalIndex
  private var partitionIDs: collection.Set[Int]                       = _
  private var writers: collection.Map[Int, EndPoint[GraphAlteration]] = _
  private var internalTotalPartitions: Int                            = _
  def totalPartitions: Int                                            = internalTotalPartitions
  private var sentUpdates: Long                                       = 0
  private val totalSourceErrors                                       = TelemetryReporter.totalSourceErrors.labels(s"$sourceID", graphID)

  def getGraphID: String         = graphID
  def getSourceID: Int           = sourceID
  def parseTuple(tuple: T): Unit = parse(this, tuple)

  private[raphtory] def getSentUpdates: Long = sentUpdates

  /** Parses `tuple` and fetches list of updates for the graph This is used internally to retrieve updates. */
  private[raphtory] def sendUpdates(tuple: T, tupleIndex: Long)(failOnError: Boolean = true): Unit =
    try {
      logger.trace(s"Parsing tuple: $tuple with index $tupleIndex")
      internalIndex = tupleIndex
      parseTuple(tuple)

    }
    catch {
      case e: Exception =>
        if (failOnError) {
          e.printStackTrace()
          println(e)
          totalSourceErrors.inc()
          throw e
        }
        else {
          logger.warn(s"Failed to parse tuple.", e.getMessage)
          totalSourceErrors.inc()
          e.printStackTrace()
        }
    }

  private[raphtory] def setupStreamIngestion(
      streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]
  ): Unit = {
    writers = streamWriters
    partitionIDs = writers.keySet
    internalTotalPartitions = writers.size
  }

  override protected def handleGraphUpdate(update: GraphUpdate): Unit = {
    logger.trace(s"handling $update")
    sentUpdates += 1
    highestTimeSeen = highestTimeSeen max update.updateTime
    val partitionForTuple = getPartitionForId(update.srcId)
    if (partitionIDs contains partitionForTuple) {
      writers(partitionForTuple).sendAsync(update)
      logger.trace(s"$update sent")
    }
  }
}
