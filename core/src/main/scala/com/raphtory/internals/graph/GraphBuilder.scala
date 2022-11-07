package com.raphtory.internals.graph

import cats.effect.std.Dispatcher
import com.raphtory.api.input._
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol.WriterService

private[raphtory] class GraphBuilderInstance[F[_], T](
    graphId: String,
    sourceId: Int,
    parse: GraphBuilder[T],
    dispatcher: Dispatcher[F],
    writers: Map[Int, WriterService[F]]
) extends Serializable
        with Graph {

  override protected def sourceID: Int = sourceId

  override protected def graphID: String = graphId

  /** Logger instance for writing out log messages */
  var highestTimeSeen: Long           = Long.MinValue
  var internalIndex: Long             = -1L
  override def index: Long            = internalIndex
  private val partitionIDs            = writers.keySet
  private val internalTotalPartitions = writers.size
  def totalPartitions: Int            = internalTotalPartitions
  private var sentUpdates: Long       = 0
  private val totalSourceErrors       = TelemetryReporter.totalSourceErrors.labels(s"$sourceID", graphID)

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

  override protected def handleGraphUpdate(update: GraphUpdate): Unit = {
    logger.trace(s"handling $update")
    sentUpdates += 1
    highestTimeSeen = highestTimeSeen max update.updateTime
    val partitionForTuple = getPartitionForId(update.srcId)
    if (partitionIDs contains partitionForTuple) {
      dispatcher.unsafeRunSync(writers(partitionForTuple).processAlteration(protocol.GraphAlteration(update)))
      logger.trace(s"$update sent")
    }
  }
}
