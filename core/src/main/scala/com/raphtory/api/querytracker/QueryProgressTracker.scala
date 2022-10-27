package com.raphtory.api.querytracker

import com.raphtory.internals.components.querymanager.JobDone
import com.raphtory.internals.components.querymanager.JobFailed
import com.raphtory.internals.components.querymanager.PerspectiveCompleted
import com.raphtory.internals.components.querymanager.PerspectiveFailed
import com.raphtory.internals.components.querymanager.PerspectiveReport
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.api.time.Perspective
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Success

abstract class ProgressTracker(jobID: String) {

  var jobDone: Boolean                          = false
  var latestPerspective: Option[Perspective]    = None
  val perspectivesList: ListBuffer[Perspective] = new ListBuffer[Perspective]()
  val perspectivesDurations: ListBuffer[Long]   = new ListBuffer[Long]()

  /** Returns job identifier for the query
    * @return job identifier
    */
  final def getJobId: String = jobID

  /** Returns the latest `Perspective` processed by the query
    * @return latest perspective
    */
  final def getLatestPerspectiveProcessed: Option[Perspective] = latestPerspective

  /** Returns list of perspectives processed for the query so far
    * @return a list of perspectives
    */
  final def getPerspectivesProcessed: List[Perspective] = perspectivesList.toList

  /** Returns the time taken to process each perspective in milliseconds */
  final def getPerspectiveDurations: List[Long] = perspectivesDurations.toList

  /** Checks if job is complete
    * @return job status
    */
  def isJobDone: Boolean = jobDone

  /** Block until job is complete */
  def waitForJob(timeout: Duration = Duration.Inf): Unit

  def handleMessage(msg: QueryManagement): Unit = {}
}

/** Tracks the progress of Raphtory queries in terms of number of perspectives processed and duration taken to process each perspective.
  * Queries in Raphtory run on a series of `Perspectives` which are how the graph would have looked at specific timestamps with given windows.
  * The progress tracker thus helps track query progress until the job is completed
  *
  * @example
  * {{{
  * import com.raphtory.Raphtory
  * import com.raphtory.FileSpout
  * import com.raphtory.algorithms.generic.ConnectedComponents
  * import com.raphtory.sinks.FileSink
  *
  * val graph = Raphtory.load(FileSpout("/path/to/your/data"), YourGraphBuilder())
  * val queryProgressTracker = graph.range(1, 32674, 10000)
  *   .window(List(500, 1000, 10000))
  *   .execute(ConnectedComponents)
  *   .writeTo(FileSink("/test_dir"))
  * val jobId                = queryProgressTracker.getJobId()
  * queryProgressTracker.waitForJob()
  * val perspectivesProcessed = queryProgressTracker.getPerspectivesProcessed()
  * }}}
  */
class QueryProgressTracker private[raphtory] (
    graphID: String,
    jobID: String,
    conf: Config
) extends ProgressTracker(jobID) {
  import QueryProgressTracker.QueryProgressTrackerException._

  private val logger: Logger                        = Logger(LoggerFactory.getLogger(this.getClass))
  private var perspectivesProcessed: Long           = 0
  private val startTime: Long                       = System.currentTimeMillis
  private var perspectiveTime: Long                 = startTime
  protected val isJobFinishedPromise: Promise[Unit] = Promise()

  // Handles message to process the `Perspective` received in case
  // the query is in progress, or `JobDone` if the query is complete
  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case perspectiveReport: PerspectiveReport =>
        val perspective           = perspectiveReport.perspective
        val perspectiveDuration   = System.currentTimeMillis() - perspectiveTime
        val windowInfo            = if (perspective.window.nonEmpty) s" with window '${perspective.window.get}'" else ""
        val (result, logFunction) = msg match {
          case _: PerspectiveCompleted      => ("finished", logger.info(_: String))
          case PerspectiveFailed(_, reason) => (s"failed (reason: '$reason')", logger.error(_: String))
        }
        val logMsg                =
          s"Job '$jobID': Perspective '${perspective.timestamp}'$windowInfo $result after $perspectiveDuration ms."
        logFunction(logMsg)

        perspectiveTime = System.currentTimeMillis
        perspectivesProcessed = perspectivesProcessed + 1

        latestPerspective = Some(perspective)
        perspectivesList.addOne(perspective)
        perspectivesDurations.addOne(perspectiveDuration)

        logger.info(s"Job $jobID: Running query, processed $perspectivesProcessed perspectives.")

      case JobDone                              =>
        logger.info(
                s"Job $jobID: Query completed with $perspectivesProcessed perspectives " +
                  s"and finished in ${System.currentTimeMillis() - startTime} ms."
        )

        jobDone = true
        isJobFinishedPromise.complete(Success(()))

      case JobFailed(error)                     =>
        isJobFinishedPromise.failure(error)
        logger.error(s"The execution of the query '$jobID' failed. Cause: '$error'")
    }

  /** Block until job is complete */
  def waitForJob(timeout: Duration = Duration.Inf): Unit =
    try Await.result[Unit](isJobFinishedPromise.future, timeout)
    catch {
      case e: DoneException =>
    }
}

object QueryProgressTracker {

  def apply(graphID: String, jobId: String, config: Config): QueryProgressTracker =
    new QueryProgressTracker(graphID, jobId, config)

  private object QueryProgressTrackerException {
    class DoneException extends Exception
  }
}
