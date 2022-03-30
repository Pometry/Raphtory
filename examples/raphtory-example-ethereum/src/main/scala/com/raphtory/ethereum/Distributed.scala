package com.raphtory.ethereum

import com.raphtory.components.spout.Spout
import com.raphtory.deploy.RaphtoryService
import com.raphtory.ethereum.graphbuilder.EthereumGraphBuilder
import com.raphtory.spouts.FileSpout
import com.typesafe.config.ConfigFactory

object Distributed extends RaphtoryService[String] {

  val config  = ConfigFactory.load("ethereum.conf").getConfig("com.raphtory.ethereum")
  val tagFile = config.getString("tagFile")

  override def defineSpout(): Spout[String] = FileSpout()

  override def defineBuilder: EthereumGraphBuilder = new EthereumGraphBuilder(tagFile)

  override def batchIngestion(): Boolean = true
}
