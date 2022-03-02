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

class QueryProgressTracker(
    jobID: String,
    conf: Config,
    pulsarController: PulsarController
) extends Component[Array[Byte]](conf: Config, pulsarController: PulsarController) {

  private val kryo                                              = PulsarKryoSerialiser()
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
            pulsarController.startQueryTrackerConsumer(Schema.BYTES, jobID, messageListener())
    )

  def stop(): Unit =
    cancelableConsumer match {
      case Some(value) =>
        pulsarController.deleteTopic(value.getTopic)
        value.close()
      case None        =>
    }

  override def handleMessage(msg: Message[Array[Byte]]): Unit =
    deserialise[QueryManagement](msg.getValue) match {

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
