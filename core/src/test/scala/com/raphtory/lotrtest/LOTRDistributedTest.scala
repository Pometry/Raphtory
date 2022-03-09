package com.raphtory.lotrtest

import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.deploy.Raphtory
import com.raphtory.deploy.RaphtoryService
import com.raphtory.spouts.FileSpout
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

object LOTRDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String] = FileSpout()

  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()

  override def batchIngestion(): Boolean = false

}
