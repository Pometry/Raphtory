package com.raphtory.api.querytracker

import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.JobDone
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.graph.Perspective
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.SynchronousQueue
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.Success

private class DoneException extends Exception

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
    jobID: String,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {
  private var perspectivesProcessed: Long = 0
  private var jobDone: Boolean            = false
  private val logger: Logger              = Logger(LoggerFactory.getLogger(this.getClass))

  private val queryTrackListener                        =
    topics.registerListener(
            s"$deploymentID-$jobID-query-tracker",
            handleMessage,
            topics.queryTrack(jobID)
    )
  private var latestPerspective: Option[Perspective]    = None
  private val perspectivesList: ListBuffer[Perspective] = new ListBuffer[Perspective]()
  private val perspectivesDurations: ListBuffer[Long]   = new ListBuffer[Long]()

  private val startTime: Long       = System.currentTimeMillis
  private var perspectiveTime: Long = startTime

  private val isJobDonePromise = Promise[Unit]()

  // Handles message to process the `Perspective` received in case
  // the query is in progress, or `JobDone` if the query is complete
  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case perspective: Perspective =>
        val perspectiveDuration = System.currentTimeMillis() - perspectiveTime

        if (perspective.window.nonEmpty)
          logger.info(
                  s"Job '$jobID': Perspective '${perspective.timestamp}' with window '${perspective.window.get}' " +
                    s"finished in $perspectiveDuration ms."
          )
        else
          logger.info(
                  s"Job '$jobID': Perspective '${perspective.timestamp}' finished in $perspectiveDuration ms."
          )

        perspectiveTime = System.currentTimeMillis
        perspectivesProcessed = perspectivesProcessed + 1

        latestPerspective = Some(perspective)
        perspectivesList.addOne(perspective)
        perspectivesDurations.addOne(perspectiveDuration)

        logger.info(s"Job $jobID: Running query, processed $perspectivesProcessed perspectives.")

      case JobDone                  =>
        logger.info(
                s"Job $jobID: Query completed with $perspectivesProcessed perspectives " +
                  s"and finished in ${System.currentTimeMillis() - startTime} ms."
        )

        jobDone = true
        isJobDonePromise.complete(Success(()))
    }

  override private[raphtory] def run(): Unit = {
    logger.info(s"Job $jobID: Starting query progress tracker.")
    queryTrackListener.start()
  }

  override private[raphtory] def stop(): Unit = {
    logger.debug(s"Stopping QueryProgressTracker for $jobID")
    queryTrackListener.close()
  }

  /** Returns job identifier for the query
    * @return job identifier
    */
  def getJobId: String =
    jobID

  /** Returns the latest `Perspective` processed by the query
    * @return latest perspective
    */
  def getLatestPerspectiveProcessed: Option[Perspective] =
    latestPerspective

  /** Returns list of perspectives processed for the query so far
    * @return a list of perspectives
    */
  def getPerspectivesProcessed: List[Perspective] =
    perspectivesList.toList

  /** Returns the time taken to process each perspective in milliseconds */
  def getPerspectiveDurations: List[Long] =
    perspectivesDurations.toList

  /** Checks if job is complete
    * @return job status
    */
  def isJobDone: Boolean =
    jobDone

  /** Block until job is complete */
  def waitForJob(timeout: Duration = Duration.Inf): Unit =
    try {
      Await.result[Unit](isJobDonePromise.future, timeout)
      if (isJobDone) stop()
    }
    catch {
      case e: DoneException =>
    }
}

object QueryProgressTracker {

  def unsafeApply(jobId: String, config: Config, topics: TopicRepository): QueryProgressTracker = {
    val tracker = new QueryProgressTracker(jobId, config, topics)
    tracker
  }
}
