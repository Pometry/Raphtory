package com.raphtory.examples.lotrTopic

import com.raphtory.deploy.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.ResourceSpout

object FileOutputRunner extends App {
  val source       = ResourceSpout("lotr.csv")
  val builder      = new LOTRGraphBuilder()
  val graph        = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val output       = FileOutputFormat("/tmp/raphtory")
  val queryHandler = graph.pointQuery(DegreesSeparation(), output, 32674)
  queryHandler.waitForJob()
}
