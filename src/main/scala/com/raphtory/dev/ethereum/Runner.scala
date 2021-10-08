package com.raphtory.dev.ethereum

import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.serialisers.DefaultSerialiser
import com.raphtory.dev.ethereum.spout.EthereumTransactionSpout
import com.raphtory.dev.ethereum.graphbuilder.EthereumGraphBuilder
import com.raphtory.dev.ethereum.analysis.TaintAlgorithm


object Runner extends App {
  val source = new EthereumTransactionSpout()
  val builder = new EthereumGraphBuilder()
  val rg = new RaphtoryGraph[EthereumTransaction](source, builder)
  val startTime = 1458700699
  val infectedNodes: Set[String] = Set("0x33e1a1ae6d566e20dc21f247126981d001b659ce")
  val graphQuery = new TaintAlgorithm(startTime, infectedNodes)
  rg.pointQuery(graphQuery,1460129610)
}
