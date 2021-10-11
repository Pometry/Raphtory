package com.raphtory.dev.ethereum.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TaintAlgorithm(startTime: Long, infectedNodes: Set[String], stopNodes: Set[String] = Set()) extends GraphAlgorithm{

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        // for each vertex in the graph
        vertex =>
          // check if it is one of our infected nodes
          if (infectedNodes contains vertex.getPropertyValue("address").get.toString){
            // set its state to tainted
            val result = List(("tainted", "startPoint", "startPoint", "startPoint"))
            // set this node as the beginning state
            vertex.setState("taintStatus", true)
            vertex.setState("taintTransactions", result)
            // tell all the other nodes that it interacts with, after starTime, that they are also infected
            // but also send the properties of when the infection happened
            vertex.getOutEdges(after = startTime).foreach(edge => edge.send(
              Tuple4("tainted", edge.getPropertyValue("hash").get.toString, edge.getPropertyValue("blockNumber").get.toString, edge.getPropertyValue("value").get.toString)
            ))
          }
      })
      .iterate({
        vertex =>
          // make an array with the status
          // fill this array with all the status messages if theres any
          val status = vertex.messageQueue[(String, String, String, String)].map(item => item._1).distinct
          // check if any of the messages are the keyword tainted
          if (status contains "tainted") {
            // check if it was previous tainted
            var state: Boolean = vertex.getOrSetState("taintStatus", false)
            // if not set the new state, which contains the sources of taint
            if (state == false) {
              val stateTxs = vertex.messageQueue[(String, String, String, String)].map(item => item)
              vertex.setState("taintTransactions", stateTxs)
              vertex.setState("taintStatus", true)
            }
            else {
              // otherwise set a brand new state
              val oldState: List[(String, String, String, String)] = vertex.getState("taintTransactions")
              val newState = state :: oldState
              vertex.setState("taintTransactions", newState)
            }

            // if we have no stop nodes set, then continue the taint spreading
            if (stopNodes.size == 0){
              vertex.getOutEdges(after = startTime).foreach(edge => edge.send(
                ("tainted", edge.getPropertyValue("hash").get.toString, edge.getPropertyValue("blockNumber").get.toString, edge.getPropertyValue("value").get.toString)
                ))
            }
            else if (!(stopNodes contains vertex.getPropertyValue("address").get.toString.toLowerCase.trim)) {
              // otherwise if we contain stop nodes and we dont have to stop then keep going
              vertex.getOutEdges(after = startTime).foreach(edge => edge.send(
                ("tainted", edge.getPropertyValue("hash").get.toString, edge.getPropertyValue("blockNumber").get.toString, edge.getPropertyValue("value").get.toString)
                ))
            }
          }
          else
            vertex.voteToHalt()
      }, 100)
      .select(vertex => Row(
        vertex.getPropertyValue("address").get.toString,
        vertex.getStateOrElse("taintStatus", false),
        vertex.getStateOrElse[Any]("taintTransactions","false")
      ))
      .filter(r=> r.get(1) == true)
      .writeTo("/tmp/taint_output")
  }
}

object TaintAlgorithm {
  def apply(startTime: Int, infectedNodes: Set[String], stopNodes: Set[String]) =
    new TaintAlgorithm(startTime, infectedNodes, stopNodes)
}