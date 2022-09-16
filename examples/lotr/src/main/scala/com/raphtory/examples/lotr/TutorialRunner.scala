package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.algorithms.generic.motif.GlobalTriangleCount
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.api.input.Type
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.language.postfixOps

object TutorialRunner extends App {

  val graph = Raphtory.newGraph()

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)

  val file = scala.io.Source.fromFile(path)
  file.getLines.foreach { line =>
    val fileLine   = line.split(",").map(_.trim)
    val sourceNode = fileLine(0)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(1)
    val tarID      = assignID(targetNode)
    val timeStamp  = fileLine(2).toLong

    graph.addVertex(timeStamp, srcID, Properties(ImmutableProperty("name", sourceNode)), Type("Character"))
    graph.addVertex(timeStamp, tarID, Properties(ImmutableProperty("name", targetNode)), Type("Character"))
    graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurence"))
  }

  graph
    .range("1/1/2020", "1/1/2021", "1 month 1 week and 1 day")
    .past()
    .execute(ConnectedComponents)
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()

  graph.destroy()

}
