package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.NodeInformation
import com.raphtory.deployment.Raphtory
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.output.JsonOutputFormat
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils

object JsonOutputRunner extends App {
  val path    = "/tmp/lotr.csv"
  val url     = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)
  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val output  = JsonOutputFormat("/tmp/raphtory")

  val queryHandler =
    graph.pointQuery(NodeInformation(initialID = 5415127257870295999L), output, 32674)
  queryHandler.waitForJob()
}
