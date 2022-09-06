package com.raphtory.api.analysis.table

import com.raphtory.api.output.sink.Sink
import com.raphtory.api.querytracker.DoneException
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.graph.Perspective
import com.raphtory.sinks.EndOutput
import com.raphtory.sinks.OutputMessages
import com.raphtory.sinks.RowOutput
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.concurrent.Promise

class TableOutput(tracker: QueryProgressTracker, topics: TopicRepository, conf: Config)
        extends Component[OutputMessages](conf) {
  private val outputDonePromise = Promise[Unit]()
  private val outputDoneFuture  = outputDonePromise.future
  private val _results          = ArrayBuffer.empty[RowOutput]
  private val logger: Logger    = Logger(LoggerFactory.getLogger(this.getClass))
  def results: Array[RowOutput] = _results.toArray

  private val outputListener =
    topics.registerListener(
            s"$graphID-$getJobId-output",
            handleMessage,
            topics.output
    )

  override def handleMessage(msg: OutputMessages): Unit =
    msg match {
      case v: RowOutput => _results.append(v)
      case EndOutput    =>
        outputDonePromise.success(())
        stop()
    }

  override private[raphtory] def run(): Unit = {
    logger.info(s"Job $getJobId: Starting output collector.")
    outputListener.start()
  }

  override private[raphtory] def stop(): Unit = {
    logger.debug(s"Stopping QueryProgressTracker for $getJobId")
    outputListener.close()
  }

  /** Returns job identifier for the query
    * @return job identifier
    */
  def getJobId: String =
    tracker.getJobId

  /** Returns the latest `Perspective` processed by the query
    * @return latest perspective
    */
  def getLatestPerspectiveProcessed: Option[Perspective] =
    tracker.getLatestPerspectiveProcessed

  /** Returns list of perspectives processed for the query so far
    * @return a list of perspectives
    */
  def getPerspectivesProcessed: List[Perspective] =
    tracker.getPerspectivesProcessed

  /** Returns the time taken to process each perspective in milliseconds */
  def getPerspectiveDurations: List[Long] =
    tracker.getPerspectiveDurations

  /** Checks if job is complete
    * @return job status
    */
  def isJobDone: Boolean =
    tracker.isJobDone

  /** Block until job is complete */
  def waitForJob(timeout: Duration = Duration.Inf): Unit =
    Await.result[Unit](outputDoneFuture, timeout)
}
