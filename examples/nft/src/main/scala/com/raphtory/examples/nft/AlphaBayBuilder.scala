package com.raphtory.examples.nft

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.Properties

import scala.util.control.NonFatal

object AlphaBayBuilder extends GraphBuilder[String] {

  override def apply(graph: Graph, tuple: String): Unit =
    try {
      val parts = tuple.split(",").map(_.trim)

      if (graph.getNEdgesAdded % (1024 * 1024) == 0)
        println(s"${graph.getNEdgesAdded}, ${System.currentTimeMillis()}")

      val src    = parts(3).trim().toLong
      val dst    = parts(4).trim().toLong
      val time   = parts(5).trim().toLong
      val valUSD = parts.last.trim().toLong

      graph.addVertex(time, src)
      graph.addVertex(time, dst)
      graph.addEdge(time, src, dst, Properties(MutableLong("price", valUSD)))
    }
    catch {
      case NonFatal(t) =>
        println(s"ERROR ON LINE $tuple")
        t.printStackTrace()
    }
}
