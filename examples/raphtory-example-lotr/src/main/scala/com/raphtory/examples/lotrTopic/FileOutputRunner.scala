package com.raphtory.examples.lotrTopic

import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout
import com.raphtory.util.FileUtils

import scala.language.postfixOps
import sys.process._
import java.io.File

object FileOutputRunner extends App {
  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  val source       = FileSpout(path)
  val builder      = new LOTRGraphBuilder()
  val graph        = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val output       = FileOutputFormat("/tmp/raphtory")
  val queryHandler = graph.pointQuery(DegreesSeparation(), output, 32674)
  queryHandler.waitForJob()
}
