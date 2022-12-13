package com.raphtory.examples.nft

import com.raphtory.api.input.{Graph, GraphBuilder, MutableLong, Properties}

import scala.util.control.NonFatal

class AlphaBayBuilder extends GraphBuilder[String]{
  override def apply(graph:Graph, tuple: String): Unit = {
    try {
      val parts = tuple.split(","); //
      if (parts.length>=6) {
        // ,txid,sourceCluster,destinationCluster,time,amount,usd
        val src = parts(3).trim().toLong
        val dst = parts(4).trim().toLong
        val time = parts(5).trim().toLong
        val valUSD = parts.last.trim().toLong

        graph.addVertex(time, src)
        graph.addVertex(time, dst)
        graph.addEdge(time, src, dst, Properties(MutableLong("valUSD", valUSD)))
      }
    } catch {
      case NonFatal(t) =>
        println(s"ERROR ON LINE ${tuple}")
        t.printStackTrace()
    }
  }
}