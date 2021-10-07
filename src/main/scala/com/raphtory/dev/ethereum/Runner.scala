package com.raphtory.dev.ethereum

import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.serialisers.DefaultSerialiser
import com.raphtory.dev.ethereum.spout.EthereumTransactionSpout
import com.raphtory.dev.ethereum.graphbuilder.EthereumGraphBuilder
import com.raphtory.dev.ethereum.analysis.TaintAlgorithm


object Runner extends App {
  val source = new EthereumTransactionSpout()
  val builder = new EthereumGraphBuilder()
  val bb = new RaphtoryGraph[EthereumTransaction](source, builder)
  val defaultSerialiser = new DefaultSerialiser()
  val startTime = 1200200
  val infectedNodes: Set[String] = Set("0x78ff59159fafb4a337fd158fcf8d9603a7c6f03a")
  val graphQuery = new TaintAlgorithm(startTime, infectedNodes)
}
