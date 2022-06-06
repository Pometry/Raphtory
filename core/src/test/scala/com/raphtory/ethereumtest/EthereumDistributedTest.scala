package com.raphtory.ethereumtest

import com.raphtory.Raphtory
import com.raphtory.RaphtoryService
import com.raphtory.api.input.Spout
import com.raphtory.spouts.FileSpout
import org.apache.pulsar.client.api.Schema

object EthereumDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String]        = FileSpout()
  override def defineBuilder: EthereumGraphBuilder = new EthereumGraphBuilder()
}
