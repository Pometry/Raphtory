package com.raphtory.examples.lotrTopic

import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.{FileSpout, ResourceSpout}

object FileOutputRunner extends App {
  // val source       = ResourceSpout("lotr.csv")
  val source  = FileSpout("/Users/Haaroony/Documents/pometry/data/lotr.csv")
  val builder      = new LOTRGraphBuilder()
  val graph        = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val output       = FileOutputFormat("/tmp/raphtoryds")
//  val queryHandler = graph.pointQuery(DegreesSeparation(), output, 32674)
//  queryHandler.waitForJob()
}
