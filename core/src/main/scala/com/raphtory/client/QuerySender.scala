package com.raphtory.client

import com.raphtory.components.querymanager.Query
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.config.ComponentFactory
import com.raphtory.config.PulsarController
import com.raphtory.config.Scheduler
import com.raphtory.serialisers.PulsarKryoSerialiser
import org.apache.pulsar.client.api.Schema

class QuerySender(
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController
) {

  val kryo                                         = PulsarKryoSerialiser()
  implicit private val schema: Schema[Array[Byte]] = Schema.BYTES

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {
    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + System.currentTimeMillis()
    val outputQuery = query.copy(name = jobID)
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(outputQuery)
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  private def getDefaultName(query: Query): String = query.hashCode().abs.toString
}
