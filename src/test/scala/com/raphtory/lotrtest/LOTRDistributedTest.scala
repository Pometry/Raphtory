package com.raphtory.lotrtest

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.instance.FileSpout
import com.raphtory.core.deploy.RaphtoryService
import com.typesafe.config.Config
import org.apache.pulsar.client.api.Schema

object LOTRDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String] = FileSpout()

  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()

  override def defineSchema(): Schema[String] = Schema.STRING

}
