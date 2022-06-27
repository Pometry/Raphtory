package com.raphtory.algorithms.temporal.dynamic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/**
  * {s}`GenericTaint((startTime: Int, infectedNodes: Iterable[Long], stopNodes: Set[Long] = Set())`
  *  : temporal tainting/infection dynamic
  *
  * A tainting/infection algorithm for a directed graph. Given the start node(s) and time
  * this algorithm will spread taint/infect any node which has received an edge after this time.
  * The following node will then infect any of its neighbours that receive an edge after the
  * time it was first infected. This repeats until the latest timestamp/iterations are met.
  *
  * ## Parameters
  *
  *  {s}`startTime: Long`
  *    : Time to start spreading taint
  *
  *  {s}`infectedNodes: Iterable[String]`
  *    : List of node names that will start as tainted
  *
  *  {s}`stopNodes: Iterable[String] = Set()`
  *    : If set, any node names that will not propagate taint
  *
  * ## States
  *
  *  {s}`taintStatus: Boolean`
  *    : {s}`true` if node is infected/tainted
  *
  *  {s}`taintHistory: List[(String, Long, Long, String)]`
  *    : List of taint messages received by the vertex. Each message has the format
  *     {s}`("tainted", edge.ID, event.time, name)`, where {s}`edge.ID` is the `ID` of the edge sending the message,
  *     {s}`event.time` is the timestamp for the tainting event, and {s}`name` is the name of the source node for the
  *     message
  *
  * ## Returns
  *
  * Table of all tainting events
  *
  *  | vertex name       | propagating edge   | time of event         | source node       |
  *  | ----------------- | ------------------ | --------------------- | ----------------- |
  *  | {s}`name: String` | {s}`edge.ID: Long` | {s}`event.time: Long` | {s}`name: String` |
  */
class GenericTaint(startTime: Long, infectedNodes: Set[String], stopNodes: Set[String] = Set()) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      // the step functions run on every single vertex ONCE at the beginning of the algorithm
      .step {
        // for each vertex in the graph
        vertex =>
          // check if it is one of our infected nodes
          if (infectedNodes contains vertex.name()) {
            // set its state to tainted
            val result = List(("tainted", -1, startTime, "startPoint"))
            // set this node as the beginning state
            vertex.setState("taintStatus", true)
            vertex.setState("taintHistory", result)
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
                              edge.send(Tuple4("tainted", edge.ID, event.time, vertex.name()))
                          )
              )
          }
      }
      .iterate(
              { vertex =>
                // check if any node has received a message
                // obtain the messages as a set
                val newMessages   =
                  vertex.messageQueue[(String, Long, Long, Long)].map(item => item).distinct
                // obtain the min time it was infected
                val infectionTime = newMessages.map(item => item._3).min
                val status        = newMessages.map(item => item._1).distinct
                // check if any of the newMessages are the keyword tainted
                if (status contains "tainted") {
                  // check if it was previous tainted
                  // if not set the new state, which contains the sources of taint
                  if (!vertex.getOrSetState("taintStatus", false)) {
                    vertex.setState("taintHistory", newMessages)
                    vertex.setState("taintStatus", true)
                  }
                  else {
                    // otherwise set a brand new state, first get the old txs it was tainted by
                    val oldState: List[(String, Long, Long, Long)] = vertex.getState("taintHistory")
                    // add the new transactions and old ones together
                    val newState                                   = List.concat(newMessages, oldState).distinct
                    // set this as the new state
                    vertex.setState("taintHistory", newState)
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
                              edge.send(Tuple4("tainted", edge.ID, event.time, vertex.name()))
                          )
                      )
                  else if (!(stopNodes contains vertex.name()))
                    // otherwise if we contain stop nodes and we dont have to stop then keep going
                    // repeat like we did in the step
                    vertex
                      .getOutEdges(after = infectionTime)
                      .foreach(edge =>
                        edge
                          .history()
                          .foreach(event =>
                            if (event.event & event.time >= infectionTime)
                              edge.send(Tuple4("tainted", edge.ID, event.time, vertex.name()))
                          )
                      )
                }
                else
                  vertex.voteToHalt()
              },
              100,
              true
      )
  // get all vertexes and their status

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(
                vertex.name(),
                vertex.getStateOrElse("taintStatus", false),
                vertex.getStateOrElse[Any]("taintHistory", "false")
        )
      )
      // filter for any that had been tainted and save to folder
      .filter(r => r.get(1) == true)
      .explode(row =>
        row
          .get(2)
          .asInstanceOf[List[(String, Long, Long, String)]]
          .map(tx => Row(row(0), tx._2, tx._3, tx._4))
      )
}

object GenericTaint {

  def apply(startTime: Int, infectedNodes: Iterable[String], stopNodes: Iterable[String] = Set()) =
    new GenericTaint(startTime, infectedNodes.toSet, stopNodes.toSet)
}
