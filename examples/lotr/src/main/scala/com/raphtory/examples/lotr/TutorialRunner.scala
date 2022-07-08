package com.raphtory.examples.lotr

import com.raphtory.Raphtory
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.table.Row
import com.raphtory.examples.lotr.graphbuilders.LOTRGraphBuilder
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout
import com.raphtory.utils.FileUtils

import scala.language.postfixOps

object TutorialRunner extends App {

  val path = "/tmp/lotr.csv"
  val url  = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv"

  FileUtils.curlFile(path, url)

  val source  = FileSpout(path)
  val builder = new LOTRGraphBuilder()
  val graph   = Raphtory.load(spout = source, graphBuilder = builder)

  val output = FileSink("/tmp/raphtory")

//  graph
//    .at(32674)
//    .past()
//    .execute(ConnectedComponents())
//    .writeTo(output)
//    .waitForJob()

  val properties = List("cclabel")

  graph
    .at(32674)
    .past()
    .step { vertex =>
      vertex.setState("cclabel", vertex.ID)
      vertex.messageAllNeighbours(vertex.ID)
    }
    .iterate(
            { vertex =>
              import vertex.IDOrdering
              val label = vertex.messageQueue[vertex.IDType].min
              if (label < vertex.getState[vertex.IDType]("cclabel")) {
                vertex.setState("cclabel", label)
                vertex.messageAllNeighbours(label)
              }
              else
                vertex.voteToHalt()
            },
            iterations = 100,
            executeMessagedOnly = true
    )
    .select { vertex =>
      val row = vertex.name() +: properties.map(name =>
        vertex.getStateOrElse(name, 0L, includeProperties = true)
      )
      Row(row: _*)
    }
    .writeTo(output)
    .waitForJob()

  graph.close()
}
