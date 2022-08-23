package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.algorithms.generic.motif.GlobalTriangleCount
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.GraphBuilder.assignID
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.api.input.Type
import com.raphtory.examples.lotr.TutorialRunner.graph
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.language.postfixOps

object TutorialRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"
  FileUtils.curlFile(path, url)

  val graph  = Raphtory.connect("test2").newGraph("test")
  val source = Source(FileSpout("/tmp/lotr.csv"), new LOTRGraphBuilder())
  graph.ingest(source)

  val graph2 = Raphtory.connect("test2").newGraph("test")

  graph
    .at(32674)
    .past()
    .execute(ConnectedComponents())
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()

  graph.destroy()

  graph2
    .at(32674)
    .past()
    .execute(PageRank())
    .writeTo(FileSink("/tmp/raphtory"))
    .waitForJob()

  graph2.destroy()

  def addLOTRData(graph: DeployedTemporalGraph) = {

    val line = scala.io.Source.fromFile(path).getLines.foreach { line =>
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
  }
}
