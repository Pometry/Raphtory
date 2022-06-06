package com.raphtory.lotrtest

import com.raphtory.Raphtory
import com.raphtory.RaphtoryService
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.FileSpout
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

object LOTRDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String]        = FileSpout()
  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
}
