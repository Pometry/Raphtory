package com.raphtory.dev.ethereum.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TaintAlgorithm(startTime: Int, infectedNodes: Set[String]) extends GraphAlgorithm{

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          if (infectedNodes contains vertex.getPropertyValue("address").getOrElse("unknown").toString){
            vertex.setState("taintStatus", true)
            vertex.getOutEdges(after = startTime).foreach(edge => edge.send("tainted"))
          }
      })
      .iterate({
        vertex =>
          val label = vertex.messageQueue[String]
          if (label == "tainted") {
            vertex.setState("taintStatus", true)
            vertex.getOutEdges(after = startTime).foreach(edge => edge.send("tainted"))
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => Row(Array(vertex.ID(),vertex.getState[String]("taintStatus"))))
      //.filter(r=> r.get(0).asInstanceOf[Long]==18174)
      .writeTo("taint_output")
  }
}

object TaintAlgorithm {
  def apply(startTime: Int, infectedNodes: Set[String]) = new TaintAlgorithm(startTime: Int, infectedNodes: Set[String])
}