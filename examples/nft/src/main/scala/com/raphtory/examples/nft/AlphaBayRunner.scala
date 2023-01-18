
import com.raphtory.RaphtoryApp
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input._
import com.raphtory.examples.nft.AlphaBayBuilder
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.spouts.FileSpout

import scala.language.postfixOps

object TutorialRunner         extends RaphtoryApp.Local with LocalRunner

trait LocalRunner { self: RaphtoryApp =>

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val path = "/home/ubuntu/data/alphabay_sorted.csv"

      //      val file = scala.io.Source.fromFile(path)
      //      file.getLines.foreach { line =>
      //        val parts = line.split(",").map(_.trim)
      //
      //        if (graph.getNEdgesAdded % (1024 * 1024) == 0)
      //          println(s"${graph.getNEdgesAdded}, ${System.currentTimeMillis()}")
      //
      //        val src   = parts(3).trim().toLong
      //        val dst   = parts(4).trim().toLong
      //        val time  = parts(5).trim().toLong
      //        val price = parts.last.trim().toLong
      //
      //        graph.addVertex(time, src)
      //        graph.addVertex(time, dst)
      //        graph.addEdge(time, src, dst, Properties(MutableLong("price", price)))
      //      }

      //The ingestion of data into a graph (line 33-45) can also be pushed into Raphtory via a Source and load function:
      val source = Source(FileSpout(path), AlphaBayBuilder)
      graph.load(source)

      // Get simple metrics
      graph
        .execute(Degree())
        .writeTo(FileSink("/tmp/raphtory"))
        .waitForJob()
    }
}