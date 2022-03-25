package com.raphtory.ethereumtest

import com.raphtory.components.spout.Spout
import com.raphtory.deployment.Raphtory
import com.raphtory.deployment.RaphtoryService
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema

object EthereumDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String] = FileSpout()

  override def defineBuilder: EthereumGraphBuilder = new EthereumGraphBuilder()

  override def batchIngestion(): Boolean = true
}
