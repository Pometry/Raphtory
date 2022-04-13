package com.raphtory.examples.lotrTopic

import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout

import scala.language.postfixOps
import sys.process._
import java.io.File

object FileOutputRunner extends App {
  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  if (!new File(path).exists())
    try s"curl -o $path $url" !!
    catch {
      case ex: Exception =>
        println(s"Failed to download 'lotr.csv' due to ${ex.getMessage}.")
        ex.printStackTrace()

        (s"rm $path" !)
        throw ex
    }
  val source       = FileSpout("/tmp/lotr.csv")
  val builder      = new LOTRGraphBuilder()
  val graph        = Raphtory.streamGraph(spout = source, graphBuilder = builder)
  val output       = FileOutputFormat("/tmp/raphtory")
  val queryHandler = graph.pointQuery(DegreesSeparation(), output, 32674)
  queryHandler.waitForJob()
}
