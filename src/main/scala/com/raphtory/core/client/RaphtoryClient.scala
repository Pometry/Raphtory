package com.raphtory.core.client

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.LiveQuery
import com.raphtory.core.components.querymanager.PointQuery
import com.raphtory.core.components.querymanager.RangeQuery
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.PulsarController
import com.raphtory.core.time.AgnosticInterval
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

private[core] class RaphtoryClient(
    private val deploymentID: String,
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController
) {

  private var internalID = deploymentID

  private val kryo                                 = PulsarKryoSerialiser()
  implicit private val schema: Schema[Array[Byte]] = Schema.BYTES
  val logger: Logger                               = Logger(LoggerFactory.getLogger(this.getClass))

  // Raphtory Client extends scheduler, queries return QueryProgressTracker, not threaded worker
  def pointQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID           = getID(graphAlgorithm)
    val agnosticWindows = windows map AgnosticInterval
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            PointQuery(jobID, graphAlgorithm, timestamp, agnosticWindows, outputFormat)
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def rangeQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID           = getID(graphAlgorithm)
    val agnosticWindows = windows map AgnosticInterval
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            RangeQuery(
                    jobID,
                    graphAlgorithm,
                    start,
                    end,
                    AgnosticInterval(increment),
                    agnosticWindows,
                    outputFormat
            )
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def liveQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID           = getID(graphAlgorithm)
    val agnosticWindows = windows map AgnosticInterval
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            LiveQuery(
                    jobID,
                    graphAlgorithm,
                    AgnosticInterval(increment),
                    agnosticWindows,
                    outputFormat
            )
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def getConfig(): Config = conf

  private def getID(algorithm: GraphAlgorithm): String =
    try {
      val path = algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size - 1) + "_" + System.currentTimeMillis()
    }
    catch {
      case e: NullPointerException => "Anon_Func_" + System.currentTimeMillis()
    }

}
