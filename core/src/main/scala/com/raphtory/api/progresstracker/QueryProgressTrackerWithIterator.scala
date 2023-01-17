package com.raphtory.api.progresstracker

import com.raphtory.api.analysis.table._
import com.raphtory.protocol.PerspectiveCompleted
import com.raphtory.protocol.PerspectiveFailed
import com.raphtory.protocol.QueryCompleted
import com.raphtory.protocol.QueryFailed
import com.raphtory.protocol.QueryUpdate
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.duration.Duration

/**
  * Iterator over perspective results with additional query tracking functionality.
  */
class QueryProgressTrackerWithIterator(
    graphID: String,
    jobID: String,
    conf: Config,
    timeout: Duration,
    header: List[String]
) extends QueryProgressTracker(graphID, jobID, conf) {

  private val logger: Logger                  = Logger(LoggerFactory.getLogger(this.getClass))
  private var outputDone: Boolean             = false
  private var jobsDone                        = 0
  private var nextResult: Option[TableOutput] = None
  private val completedResults                = new LinkedBlockingQueue[Any]

  override def handleQueryUpdate(msg: QueryUpdate): Unit = {
    msg match {
      case PerspectiveCompleted(perspective, rows, _) =>
        logger.debug(s"received message $msg")
        completedResults.add(TableOutput(getJobId, perspective, header, rows.toArray, conf))
      case _: QueryCompleted | _: QueryFailed         => // QueryCompleted or QueryFailed
        completedResults.add(msg)
      case PerspectiveFailed(perspective, reason, _)  =>
        logger.error(s"perspective $perspective failed: $reason")
        completedResults.add(TableOutput(getJobId, perspective, header, Array.empty[Row], conf))
      case _                                          =>
        logger.error(s"unknown $msg")
    }
    super.handleQueryUpdate(msg)
  }

  override def isJobDone: Boolean = outputDone

  private def waitForNextResult(): Any = {
    logger.debug("waiting for result")
    if (timeout.isFinite)
      Option(completedResults.poll(timeout.length, timeout.unit))
    else
      Option(completedResults.take())
  }

  object TableOutputIterator extends Iterator[TableOutput] {
    import TableOutputIterator.TableOutputIteratorException._

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
              case QueryFailed(reason, _) =>
                throw new JobFailedException(jobID, reason)
              case QueryCompleted(_)      =>
                outputDone = true
                logger.debug("hasNext false as output complete after waiting for result")
                false
              case v: TableOutput         =>
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

    private object TableOutputIteratorException {

      class JobFailedException(jobID: String, reason: String)
              extends RuntimeException(s"The execution of the query '$jobID' failed. Reason: $reason")
    }
  }
}

object QueryProgressTrackerWithIterator {

  def apply(
      graphID: String,
      jobID: String,
      conf: Config,
      timeout: Duration,
      header: List[String]
  ): QueryProgressTrackerWithIterator =
    new QueryProgressTrackerWithIterator(graphID, jobID, conf, timeout, header)
}
