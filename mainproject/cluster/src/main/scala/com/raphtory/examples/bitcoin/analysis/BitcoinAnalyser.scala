package com.raphtory.examples.bitcoin.analysis

import akka.actor.ActorContext
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.controller.GraphRepoProxy

class BitcoinAnalyser extends Analyser {
  override implicit var context: ActorContext = _
  override implicit var managerCount: Int = _

  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = {
    var results = Vector.empty[(String, Double)]
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v)
      if(vertex.getPropertyCurrentValue("type").getOrElse("no-type").equals("address")) {
        val address = vertex.getPropertyCurrentValue("address").getOrElse("no address")
        var total: Double = 0
        for (edge <- vertex.getIngoingNeighbors) {
          val edgeValue = vertex.getIngoingNeighborProp(edge, "value").getOrElse("0")
          total += edgeValue.toDouble
        }
        results :+= (address, total)
      }
    })
    //println("Sending step end")
    results.sortBy(f => f._2)(Ordering[Double].reverse).take(10)
  }

  override def setup()(implicit proxy: GraphRepoProxy.type): Any = {

  }
}



