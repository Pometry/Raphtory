package com.raphtory.api.analysis.table

import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.api.time.Perspective
import com.raphtory.internals.components.output.EndOutput
import com.raphtory.internals.components.output.EndPerspective
import com.raphtory.internals.components.output.OutputMessages
import com.raphtory.internals.components.output.RowOutput
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/** Iterator over perspective results with additional query tracking functionality */
case class TableOutputTracker(tracker: QueryProgressTracker, topics: TopicRepository, conf: Config, timeout: Duration)
        extends Component[OutputMessages](conf)
        with Iterator[TableOutput] {
  private var outputDone: Boolean             = false
  private var jobsDone                        = 0
  private var nextResult: Option[TableOutput] = None
  private val resultsInProgress               = mutable.Map.empty[Perspective, ArrayBuffer[Row]]
  private val perspectiveDoneCounts           = mutable.Map.empty[Perspective, Int]
  private val logger: Logger                  = Logger(LoggerFactory.getLogger(this.getClass))
  private val completedResults                = new LinkedBlockingQueue[Any]

  private val outputListener =
    topics.registerListener(
            s"$graphID-$getJobId-output",
            handleMessage,
            topics.output(getJobId)
    )

  override def handleMessage(msg: OutputMessages): Unit = {
    logger.debug(s"received message $msg")
    msg match {
      case RowOutput(perspective, row) =>
        resultsInProgress.getOrElseUpdate(perspective, ArrayBuffer.empty[Row]).append(row)
      case EndPerspective(perspective) =>
        perspectiveDoneCounts(perspective) = perspectiveDoneCounts.getOrElse(perspective, 0) + 1
        if (perspectiveDoneCounts(perspective) == totalPartitions) {
          perspectiveDoneCounts.remove(perspective)
          resultsInProgress.remove(perspective).map(_.toArray) match {
            case Some(rows) =>
              completedResults.add(TableOutput(getJobId, perspective, rows, conf, topics))
            case None       =>
              completedResults.add(TableOutput(getJobId, perspective, Array.empty, conf, topics))
          }
        }
      case EndOutput                   =>
        jobsDone += 1
        if (jobsDone == totalPartitions) {
          completedResults.add(EndOutput)
          stop()
        }
    }
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
    outputDone

  private def waitForNextResult(): Any = {
    logger.debug("waiting for result")
    if (timeout.isFinite)
      Option(completedResults.poll(timeout.length, timeout.unit))
    else
      Option(completedResults.take())
  }

  override def hasNext: Boolean =
    if (nextResult.isDefined) {
      logger.debug("hasNext true as result already present")
      true
    }
    else if (outputDone) {
      logger.debug("hasNext false as output complete")
      false
    }
    else
      waitForNextResult() match {
        case Some(res) =>
          res match {
            case EndOutput      =>
              outputDone = true
              logger.debug("hasNext false as output complete after waiting for result")
              false
            case v: TableOutput =>
              nextResult = Some(v)
              logger.debug("hasNext true as new result available after waiting")
              true
          }
        case None      =>
          false
      }

  override def next(): TableOutput =
    if (hasNext) {
      val out = nextResult.get
      nextResult = None
      out
    }
    else if (outputDone)
      throw new NoSuchElementException("All perspectives already processed")
    else
      throw new NoSuchElementException("Timed out while waiting for next perspective")
}
