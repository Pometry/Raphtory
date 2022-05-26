package com.raphtory.components.querytracker

import com.raphtory.communication.TopicRepository
import com.raphtory.components.Component
import com.raphtory.components.querymanager.JobDone
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.graph.Perspective
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Tracks the progress of Raphtory queries in terms of number of perspectives processed and duration taken to process each perspective.
  * Queries in Raphtory run as a series of `Perspectives` which are graph views at specific timestamps and windows as the query progresses.
  * The progress tracker thus helps track query progress until the job is completed. Query types supported include `PointQuery`, `RangeQuery` and `LiveQuery`
  *
  * Usage:
  *
  * {{{
  * import com.raphtory.deployment.Raphtory
  * import com.raphtory.lotrtest.LOTRGraphBuilder
  * import com.raphtory.components.spout.instance.ResourceSpout
  * import com.raphtory.GraphState
  * import com.raphtory.output.FileOutputFormat
  *
  * val graph = Raphtory.batchLoadGraph(ResourceSpout("resource"), LOTRGraphBuilder())
  * val queryProgressTracker = graph.rangeQuery(GraphState(),FileOutputFormat("/test_dir"),1, 32674, 10000, List(500, 1000, 10000))
  * val jobId                = queryProgressTracker.getJobId()
  * queryProgressTracker.waitForJob()
  * val perspectivesProcessed = queryProgressTracker.getPerspectivesProcessed()
  * }}}
  */

class DoneException extends Exception

class QueryProgressTracker(
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

  private val isJobDoneFuture = Task
    .never[Unit]
    .doOnCancel(
            Task.eval[Unit] {
              stop()
            }
    )
    .onCancelRaiseError(
            new DoneException
    ) // see this for why this is necessary https://github.com/monix/monix/issues/860
    .runToFuture

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
        isJobDoneFuture.cancel()
    }

  override def run(): Unit = {
    logger.info(s"Job $jobID: Starting query progress tracker.")
    queryTrackListener.start()
  }

  override def stop(): Unit = {
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

  /** Block until job is complete, repeats check every second */
  def waitForJob(timeout: Duration = Duration.Inf): Unit =
    try Await.result[Unit](isJobDoneFuture, timeout)
    catch {
      case e: DoneException =>
    }
}
