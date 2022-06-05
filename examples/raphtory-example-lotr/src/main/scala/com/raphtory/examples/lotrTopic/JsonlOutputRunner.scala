package com.raphtory.examples.lotrTopic

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.NodeInformation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.formats.JsonlFormat
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.util.FileUtils

object JsonlOutputRunner extends App {
  val path     = "/tmp/lotr.csv"
  val url      = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)
  val source   = FileSpout(path)
  val builder  = new LOTRGraphBuilder()
  val graph    = Raphtory.stream(spout = source, graphBuilder = builder)
  val filepath = "/tmp/gsonLotrOutput"
  val output   = FileSink(filepath, format = JsonlFormat())

  val queryHandler =
    graph
      .at(32674)
      .past()
      .execute(NodeInformation(initialID = 5415127257870295999L))
      .writeTo(output)

  queryHandler.waitForJob()
}
