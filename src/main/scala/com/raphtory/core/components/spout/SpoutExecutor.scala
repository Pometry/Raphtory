package com.raphtory.core.components.spout

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Message

abstract class SpoutExecutor[S](
    conf: Config,
    private val pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[S, Nothing](conf: Config, pulsarController: PulsarController, scheduler) {
  override def handleMessage(msg: Nothing): Boolean = false
  def name(): String                                = "Spout"

}
