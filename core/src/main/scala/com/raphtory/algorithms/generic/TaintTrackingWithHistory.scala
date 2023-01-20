package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

class TaintTrackingWithHistory(startTime: Long, infectedNodes: Set[String], stopNodes: Set[String] = Set())
        extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    graph.reducedView
      // the step functions run on every single vertex ONCE at the beginning of the algorithm
      .step {
        // for each vertex in the graph
        vertex =>
          // check if it is one of our infected nodes
          if (infectedNodes contains vertex.getPropertyOrElse("address", "").toString) {
            // set its state to tainted
            val result = List(("tainted", "startPoint", startTime, "startPoint", "startPoint"))
            // set this node as the beginning state
            vertex.setState("taintStatus", true)
            vertex.setState("taintTransactions", result)
            // tell all the other nodes that it interacts with, after starTime, that they are also infected
            // but also send the properties of when the infection happened
            vertex
              .getOutEdges(after = startTime)
              .foreach(
                      // for every edge/transaction this node made after the time
                      edge =>
                        // get all the transactions that this node sent
                        edge
                          .history()
                          .foreach(event =>
                            if (event.event & event.time >= startTime)
                              // if it was a sent transaction and the time is after the start, mark as an edge
                              edge.send(
                                      Tuple5(
                                              "tainted",
                                              edge.getPropertyAt("hash", event.time).getOrElse(""),
                                              event.time,
                                              edge.getPropertyAt("value", event.time).getOrElse(""),
                                              vertex
                                                .getPropertyAt("address", event.time)
                                                .getOrElse("")
                                      )
                              )
                          )
              )
          }
      }
      .iterate(
              { vertex =>
                // check if any node has received a message
                // obtain the messages as a set
                val newMessages   = vertex
                  .messageQueue[(String, String, Long, String, String)]
                  .map(item => item)
                  .distinct
                // obtain the min time it was infected
                val infectionTime = newMessages.map(item => item._3).min
                val status        = newMessages.map(item => item._1).distinct
                // check if any of the newMessages are the keyword tainted
                if (status contains "tainted") {
                  // check if it was previous tainted
                  // if not set the new state, which contains the sources of taint
                  if (vertex.getOrSetState("taintStatus", false) == false) {
                    vertex.setState("taintTransactions", newMessages)
                    vertex.setState("taintStatus", true)
                  }
                  else {
                    // otherwise set a brand new state, first get the old txs it was tainted by
                    val oldState: List[(String, String, Long, String, String)] =
                      vertex.getState("taintTransactions")
                    // add the new transactions and old ones together
                    val newState                                               = List.concat(newMessages, oldState).distinct
                    // set this as the new state
                    vertex.setState("taintTransactions", newState)
                  }
                  // if we have no stop nodes set, then continue the taint spreading
                  if (stopNodes.size == 0)
                    // repeat like we did in the step
                    vertex
                      .getOutEdges(after = infectionTime)
                      .foreach(edge =>
                        edge
                          .history()
                          .foreach(event =>
                            if (event.event & event.time >= infectionTime)
                              edge.send(
                                      Tuple5(
                                              "tainted",
                                              edge.getPropertyAt("hash", event.time).getOrElse(""),
                                              event.time,
                                              edge.getPropertyAt("value", event.time).getOrElse(""),
                                              vertex
                                                .getPropertyAt("address", event.time)
                                                .getOrElse("")
                                      )
                              )
                          )
                      )
                  else if (
                          !(stopNodes contains vertex
                            .getPropertyOrElse("address", "")
                            .toString
                            .toLowerCase
                            .trim)
                  )
                    // otherwise if we contain stop nodes and we dont have to stop then keep going
                    // repeat like we did in the step
                    vertex
                      .getOutEdges(after = infectionTime)
                      .foreach(edge =>
                        edge
                          .history()
                          .foreach(event =>
                            if (event.event & event.time >= infectionTime)
                              edge.send(
                                      Tuple5(
                                              "tainted",
                                              edge.getPropertyAt("hash", event.time).getOrElse(""),
                                              event.time,
                                              edge.getPropertyAt("value", event.time).getOrElse(""),
                                              vertex
                                                .getPropertyAt("address", event.time)
                                                .getOrElse("")
                                      )
                              )
                          )
                      )
                }
                else
                  vertex.voteToHalt()
              },
              100,
              true
      )

  override def tabularise(graph: ReducedGraphPerspective): Table =
    graph
      .select("address", "taintStatus", "taintTransactions")
      // filter for any that had been tainted and save to folder
      .filter(r => r.get("taintStatus") == true)
      .explode(row =>
        row
          .get("taintTransactions")
          .asInstanceOf[List[(String, String, Long, String, String)]]
          .map(tx =>
            Row(
                    "tainted" -> row.get("tainted"),
                    "address" -> tx._5,
                    "time"    -> tx._3,
                    "hash"    -> tx._2,
                    "value"   -> tx._4
            )
          )
      )
}

object TaintTrackingWithHistory {

  def apply(startTime: Int, infectedNodes: Set[String], stopNodes: Set[String]) =
    new TaintTrackingWithHistory(startTime, infectedNodes, stopNodes)
}
