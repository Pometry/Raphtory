package com.raphtory.examples.lotr

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.utils.FileUtils
import scala.language.postfixOps

object LocalRunner extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph(destroy = true) { graph =>
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
        graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurrence"))
      }

      //Get simple metrics
      graph
        .execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //PageRank
      graph
        .at(32674)
        .past()
        .transform(PageRank())
        .execute(NodeList(Seq("prlabel")))
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //Connected Components
      graph
        .at(32674)
        .past()
        .execute(ConnectedComponents)
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //Chained Example
      graph
        .at(32674)
        .past()
        .transform(PageRank())
        .transform(ConnectedComponents)
        .transform(Degree())
        .execute(NodeList(Seq("prlabel", "cclabel", "inDegree", "outDegree", "degree")))
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

    }
}

object RemoteRunner extends RaphtoryApp.Remote("localhost", 1736) {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph(destroy = true) { graph =>
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
        graph.addEdge(timeStamp, srcID, tarID, Type("Character Co-occurrence"))
      }

      //Get simple metrics
      graph
        .execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //PageRank
      graph
        .at(32674)
        .past()
        .transform(PageRank())
        .execute(NodeList(Seq("prlabel")))
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //Connected Components
      graph
        .at(32674)
        .past()
        .execute(ConnectedComponents)
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      //Chained Example
      graph
        .at(32674)
        .past()
        .transform(PageRank())
        .transform(ConnectedComponents)
        .transform(Degree())
        .execute(NodeList(Seq("prlabel", "cclabel", "inDegree", "outDegree", "degree")))
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

    }
}
