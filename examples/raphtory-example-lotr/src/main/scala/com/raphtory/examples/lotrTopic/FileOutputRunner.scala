package com.raphtory.examples.lotrTopic

import com.raphtory.core.components.spout.instance.ResourceSpout
import com.raphtory.output.FileOutputFormat
import com.raphtory.core.client.RaphtoryGraph
import com.raphtory.core.deploy.Raphtory
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder

object FileOutputRunner extends App {
  val source = ResourceSpout("lotr.csv")
  val builder = new LOTRGraphBuilder()
  val graph = Raphtory.createGraph(spout = source, graphBuilder = builder)
  val output = FileOutputFormat("/tmp/raphtory")
  val queryHandler = graph.pointQuery(DegreesSeparation(), output, 32674)
  queryHandler.waitForJob()
}
