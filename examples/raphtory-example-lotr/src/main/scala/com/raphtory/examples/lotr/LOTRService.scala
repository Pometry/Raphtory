package com.raphtory.examples.lotr

import com.raphtory.api.input.Spout
import com.raphtory.RaphtoryService
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout

object LOTRService extends RaphtoryService[String] {

  override def defineSpout(): Spout[String] = FileSpout("/tmp/lotr.csv")

  override def defineBuilder: LOTRGraphBuilder = new LOTRGraphBuilder()

}
