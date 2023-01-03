package com.raphtory.examples.nft

import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.{MutableLong, Properties}
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink

import scala.language.postfixOps


object TutorialRunner      extends RaphtoryApp.Local with LocalRunner2
object XXXArrowTutorialRunner extends RaphtoryApp.ArrowLocal[VertexProps, EdgeProps] with LocalRunner2

trait LocalRunner2 { self: RaphtoryApp =>

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val path = "/home/jatinder/projects/Pometry/arrow-core/alphabay_sorted.csv"

      val file = scala.io.Source.fromFile(path)
      file.getLines.foreach { line =>
        val parts   = line.split(",").map(_.trim)

        if (graph.getNEdgesAdded % (1024*1024) == 0) {
          println(graph.getNEdgesAdded);
        }
        val src = parts(3).trim().toLong
        val dst = parts(4).trim().toLong
        val time = parts(5).trim().toLong
        val price = parts.last.trim().toLong

        graph.addVertex(time, src)
        graph.addVertex(time, dst)
        graph.addEdge(time, src, dst, Properties(MutableLong("price", price)))
      }

      //The ingestion of data into a graph (line 33-45) can also be pushed into Raphtory via a Source and load function:
      //      val source = Source(FileSpout(path), LotrGraphBuilder)
      //      graph.load(source)

      // Get simple metrics
      graph.slice(Long.MinValue, Long.MaxValue)
        .execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()

      println(graph.getNEdgesAdded)
      println(graph.getNVerticesAdded)

    }

}

case class VertexProps()
case class EdgeProps(price: Long)