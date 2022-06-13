package com.raphtory.lotrtest

import com.raphtory.RaphtoryService
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.spouts.FileSpout

object LOTRDistributedTest extends RaphtoryService[String] {
  override def defineSpout(): Spout[String]        = FileSpout()
  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
}
