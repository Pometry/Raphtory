package com.raphtory.dev.ethereum.analysis

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

class TaintAlgorithm(startTime: Long, infectedNodes: Set[String], stopNodes: Set[String] = Set()) extends GraphAlgorithm{

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      // the step functions run on every single vertex ONCE at the beginning of the algorithm
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
            vertex.getOutEdges(after = startTime).foreach(edge =>
                Tuple5("tainted",
                  edge.getPropertyValueAt("hash", edge.firstActivityAfter(startTime).time),
                  edge.firstActivityAfter(startTime).time,
                  edge.getPropertyValueAt("value", edge.firstActivityAfter(startTime).time),
                  vertex.getPropertyValueAt("address", edge.firstActivityAfter(startTime).time),
              ))
          }
      })
      .iterate({
        vertex =>
          // make an array with the status
          // fill this array with all the status newMessages if theres any
          if (vertex.hasMessage()) {
            val newMessages = vertex.messageQueue[(String, String, Long, String,String)].map(item => item)
            val infectionTime = newMessages.map(item => item._3).min
            val status = newMessages.map(item => item._1).distinct
            // check if any of the newMessages are the keyword tainted
            if (status contains "tainted") {
              // check if it was previous tainted
              // if not set the new state, which contains the sources of taint
              if (vertex.getOrSetState("taintStatus", false) == false) {
                vertex.setState("taintTransactions", newMessages)
                vertex.setState("taintStatus", true)
              }
              else {
                // otherwise set a brand new state
                val oldState: List[(String, String, Long, String, String)] = vertex.getState("taintTransactions")
                val newState = List.concat(newMessages, oldState).distinct
                vertex.setState("taintTransactions", newState)
              }
              // if we have no stop nodes set, then continue the taint spreading
              if (stopNodes.size == 0) {
                vertex.getOutEdges(after = infectionTime).foreach(edge => edge.send({
                  ("tainted",
                    edge.getPropertyValue("hash").get.toString,
                    edge.firstActivityAfter(infectionTime).time,
                    edge.getPropertyValue("value").get.toString,
                    vertex.getPropertyValue("address").get.toString)
                }
                ))
              }
              else if (!(stopNodes contains vertex.getPropertyValue("address").get.toString.toLowerCase.trim)) {
                // otherwise if we contain stop nodes and we dont have to stop then keep going
                vertex.getOutEdges(after = infectionTime).foreach(edge => edge.send({
                  ("tainted",
                    edge.getPropertyValue("hash").get.toString,
                    edge.firstActivityAfter(infectionTime).time,
                    edge.getPropertyValue("value").get.toString,
                    vertex.getPropertyValue("address").get.toString)
                }))
              }
            }
            else
              vertex.voteToHalt()
          }
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