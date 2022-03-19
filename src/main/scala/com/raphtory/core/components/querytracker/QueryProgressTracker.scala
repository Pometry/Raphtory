package com.raphtory.core.components.querytracker

import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.JobDone
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.Perspective
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import scala.collection.mutable.ListBuffer


/**
  * {s}`QueryProgressTracker`
  *  : {s}`QueryProgressTracker` is used to track the progress of Raphtory queries in terms of number of perspectives processed and duration taken to process each perspective.
  *    Queries in Raphtory run as a series of {s}`Perspectives` which are graph views at specific timestamps and windows as the query progresses.
  *    The progress tracker thus helps track query progress until the job is completed. Query types supported include {s}`PointQuery`, {s}`RangeQuery` and {s}`LiveQuery`
  *
  * ## Methods
  *
  *   {s}`getJobId(): String`
  *    : Returns job identifier for the query
  *
  *   {s}`getLatestPerspectiveProcessed(): Perspective`
  *    : Returns the latest {s}`Perspective` processed by the query
  *
  *   {s}`getPerspectivesProcessed(): List[Perspective]`
  *    : Returns list of perspectives processed for the query till the current timestamp
  *
  *   {s}`getPerspectiveDurations(): List[Long]`
  *    : Returns the time duration taken to process each of perspectives till the current timestamp
  *
  *   {s}`isJobDone(): Boolean`
  *    : Checks if job is complete
  *
  *   {s}`waitForJob()`
  *    : Polling mechanism to checks if the job is complete, repeats check after every second
  *
  *   {s}`handleMessage(msg: QueryManagement): Unit`
  *    : Handles message to process the {s}`Perspective` received in case the query is in progress, or {s}`JobDone` if the query is complete
  *
  *   {s}`run(): Unit`
  *    : Starts the query tracker pulsar consumer
  *
  *   {s}`stop(): Unit`
  *    : Stops the query tracker consumer
  *
  *
  * Example Usage:
  *
  * ```{code-block} scala
  *
  * import com.raphtory.core.deploy.Raphtory
  * import com.raphtory.lotrtest.LOTRGraphBuilder
  * import com.raphtory.core.components.spout.instance.ResourceSpout
  * import com.raphtory.GraphState
  * import com.raphtory.output.FileOutputFormat
  *
  * val customConfig = Map(("raphtory.pulsar.endpoint", "localhost:1234"))
  * Raphtory.createClient("deployment123", customConfig)
  * val graph = Raphtory.createGraph(ResourceSpout("resource"), LOTRGraphBuilder())
  * val queryProgressTracker = graph.rangeQuery(GraphState(),FileOutputFormat("/test_dir"),1, 32674, 10000, List(500, 1000, 10000))
  * val jobId                = queryProgressTracker.getJobId()
  * Thread.sleep(2000)
  * val perspectivesProcessed = queryProgressTracker.getPerspectivesProcessed()
  * queryProgressTracker.waitForJob()
  *
  * ```
  *
  */
class QueryProgressTracker(
    jobID: String,
    conf: Config,
    pulsarController: PulsarController
) extends Component[QueryManagement](conf: Config, pulsarController: PulsarController) {
  implicit private val schema: Schema[Array[Byte]]              = Schema.BYTES
  private var perspectivesProcessed: Long                       = 0
  private var jobDone: Boolean                                  = false
  private var perspectivesList: ListBuffer[Perspective]         = new ListBuffer[Perspective]()
  private var perspectivesDurations: ListBuffer[Long]           = new ListBuffer[Long]()
  private var latestPerspective: Perspective                    = null
  private var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  logger.info("Starting query progress tracker.")

  val startTime: Long       = System.currentTimeMillis //fetch starting time
  var perspectiveTime: Long = startTime

  def getJobId(): String =
    jobID

  def getLatestPerspectiveProcessed(): Perspective =
    latestPerspective

  def getPerspectivesProcessed(): List[Perspective] = {
    s"PROGRESS TRACKER: RUNNING QUERY FOR $jobID, PROCESSED $perspectivesProcessed PERSPECTIVES!"
    perspectivesList.toList
  }

  def getPerspectiveDurations(): List[Long] =
    perspectivesDurations.toList

  def isJobDone(): Boolean =
    jobDone

  def waitForJob() =
    //TODO as executor sleep
    while (!jobDone)
      Thread.sleep(1000)

  override def run(): Unit =
    cancelableConsumer = Some(
            pulsarController.startQueryTrackerConsumer(jobID, messageListener())
    )

  def stop(): Unit =
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {

      case p: Perspective =>
        val perspectiveDuration = System.currentTimeMillis() - perspectiveTime

        if (p.window.nonEmpty)
          logger.info(
                  s"Job '$jobID': Perspective '${p.timestamp}' with window '${p.window.get}' finished in $perspectiveDuration ms."
          )
        else
          logger.info(
                  s"Job '$jobID': Perspective '${p.timestamp}' finished in $perspectiveDuration ms."
          )

        perspectiveTime = System.currentTimeMillis
        perspectivesProcessed += 1
        latestPerspective = p
        perspectivesList += p
        perspectivesDurations += perspectiveDuration

        logger.info(s"Job $jobID: Running query, processed $perspectivesProcessed perspectives.")

      case JobDone        =>
        logger.info(
                s"Job $jobID: Query completed with $perspectivesProcessed perspectives " +
                  s"and finished in ${System.currentTimeMillis() - startTime} ms."
        )

        jobDone = true

      // TODO Need to re-enable below
      // close consumer
      // stop()
    }
}
