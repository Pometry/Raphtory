package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema

class IdentitySpoutExecutor[T](
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends SpoutExecutor[T](conf: Config, pulsarController: PulsarController, scheduler: Scheduler) {

  override def stop(): Unit = {}
  override def run(): Unit = {}
}
