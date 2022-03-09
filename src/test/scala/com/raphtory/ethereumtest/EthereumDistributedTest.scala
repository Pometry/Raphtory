package com.raphtory.ethereumtest

import com.raphtory.core.components.spout.Spout
import com.raphtory.core.deploy.Raphtory
import com.raphtory.core.deploy.RaphtoryService
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema

object EthereumDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String] = FileSpout()

  override def defineBuilder: EthereumGraphBuilder = new EthereumGraphBuilder()
}
