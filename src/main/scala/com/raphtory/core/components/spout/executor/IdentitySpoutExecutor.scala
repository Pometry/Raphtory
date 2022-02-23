package com.raphtory.core.components.spout.executor

import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config

class IdentitySpoutExecutor[T](conf: Config, pulsarController: PulsarController)
        extends SpoutExecutor[T](conf: Config, pulsarController: PulsarController) {
  override def stop(): Unit = {}
  override def run(): Unit  = {}
}
