package com.raphtory.dev.ethereum

import com.raphtory.core.build.server.RaphtoryGraph
import com.raphtory.serialisers.DefaultSerialiser
import com.raphtory.dev.ethereum.spout.EthereumTransactionSpout
import com.raphtory.dev.ethereum.graphbuilder.EthereumGraphBuilder

object Runner extends App {
  val source = new EthereumTransactionSpout()
  val builder = new EthereumGraphBuilder()
  val bb = new RaphtoryGraph[EthereumTransaction](source, builder)
  val defaultSerialiser = new DefaultSerialiser()
}
