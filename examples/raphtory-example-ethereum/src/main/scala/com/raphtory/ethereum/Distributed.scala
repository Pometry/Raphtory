package com.raphtory.ethereum

import com.raphtory.components.spout.Spout
import com.raphtory.deployment.RaphtoryService
import com.raphtory.ethereum.graphbuilder.EthereumGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils
import com.typesafe.config.ConfigFactory

object Distributed extends RaphtoryService[String] {

  val path = "/tmp/etherscan_tags.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/etherscan_tags.csv"
  FileUtils.curlFile(path, url)

  override def defineSpout(): Spout[String] = FileSpout("/tmp/etherscan_tags.csv")

  override def defineBuilder: EthereumGraphBuilder = new EthereumGraphBuilder()

  override def batchIngestion(): Boolean = true
}
