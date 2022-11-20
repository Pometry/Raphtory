package com.raphtory.examples.nft

import com.raphtory.api.input.{Graph, GraphBuilder, LongProperty, Properties}

class AlphaBayBuilder extends GraphBuilder[String]{
  override def apply(graph:Graph, tuple: String): Unit = {
    val parts = tuple.split(",").map(_.trim)
    // ,txid,sourceCluster,destinationCluster,time,amount,usd
    val src = parts(3).toLong
    val dst = parts(4).toLong
    val time = parts(5).toLong
    val valUSD = parts.last.toLong

    graph.addVertex(time,src)
    graph.addVertex(time,dst)
    graph.addEdge(time,src,dst, Properties(LongProperty("valUSD", valUSD)))
  }
}