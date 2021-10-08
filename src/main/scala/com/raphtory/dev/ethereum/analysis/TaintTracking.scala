package com.raphtory.dev.ethereum.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TaintAlgorithm(startTime: Long, infectedNodes: Set[String]) extends GraphAlgorithm{

  println("NODES THAT ARE INFECTED ->")
  println(infectedNodes)
  println("*****")

  override def algorithm(graph: GraphPerspective): Unit = {
    println("Running algorithm")
    println(infectedNodes)
    graph
      .step({
        vertex =>
          println(infectedNodes.size)
          if (infectedNodes contains vertex.getPropertyValue("address").get.toString){
            vertex.setState("taintStatus", true)
            vertex.getOutEdges(after = startTime).foreach(edge => edge.send("tainted"))
            println("**********Node found")
          }
          println(vertex.getPropertyValue("address").get.toString)
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
      .select(vertex => Row(vertex.ID(),vertex.getStateOrElse[String]("taintStatus","clean")))
      //.filter(r=> ! (r.getString(1) equals "clean"))
      .writeTo("/tmp/taint_output")
  }
}

object TaintAlgorithm {
  def apply(startTime: Int, infectedNodes: Set[String]) = new TaintAlgorithm(startTime: Int, infectedNodes: Set[String])
}