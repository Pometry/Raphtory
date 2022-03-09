package com.raphtory.core.components.spout

import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Message
import scala.reflect.runtime.universe.TypeTag

/** @DoNotDocument */
abstract class SpoutExecutor[T](
    conf: Config,
    private val pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[T](conf: Config, pulsarController: PulsarController) {
  protected val failOnError: Boolean = conf.getBoolean("raphtory.spout.failOnError")

  override def handleMessage(msg: T): Unit = {}

}
