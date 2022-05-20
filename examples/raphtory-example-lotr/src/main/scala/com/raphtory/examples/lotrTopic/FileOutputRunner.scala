package com.raphtory.examples.lotrTopic

import com.raphtory.deployment.Raphtory
import com.raphtory.output.FileOutputFormat
import com.raphtory.examples.lotrTopic.analysis.DegreesSeparation
import com.raphtory.examples.lotrTopic.graphbuilders.LOTRGraphBuilder
import com.raphtory.spouts.FileSpout
import com.raphtory.algorithms.api.Chain
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.{Degree, WeightedDegree}
import com.raphtory.algorithms.generic.filters.{EdgeQuantileFilter, VertexQuantileFilter}
import com.raphtory.util.FileUtils

import scala.language.postfixOps
import sys.process._
import java.io.File

object FileOutputRunner extends App {
  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.stream(spout = source, graphBuilder = builder)
  val output  = FileOutputFormat("/tmp/raphtory")

    val queryHandler = graph
      .at(32674)
      .past()
      .execute(Chain(EdgeQuantileFilter[Int](lower = 0.5f, weightString = "weight"),ConnectedComponents()))
      .writeTo(output)

    queryHandler.waitForJob()
}
