package com.raphtory.examples.lotrTopic

import com.raphtory.algorithms.generic.NodeInformation
import com.raphtory.deployment.Raphtory
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.output.GsonOutputFormat
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils

object GsonOutputRunner extends App {
  val path     = "/tmp/lotr.csv"
  val url      = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)
  val source   = FileSpout(path)
  val builder  = new LOTRGraphBuilder()
  val graph    = Raphtory.stream(spout = source, graphBuilder = builder)
  val filepath = "/tmp/gsonLotrOutput"
  val output   = GsonOutputFormat(filepath)

  val queryHandler =
    graph
      .at(32674)
      .past()
      .execute(NodeInformation(initialID = 5415127257870295999L))
      .writeTo(output)

  queryHandler.waitForJob()
}
