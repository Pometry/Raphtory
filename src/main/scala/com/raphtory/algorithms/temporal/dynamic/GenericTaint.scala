package com.raphtory.algorithms.temporal.dynamic

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

/**
Description
  A tainting/infection algorithm for a directed graph. Given the start node(s) and time
  this algorithm will spread taint/infect any node which has received an edge after this time.
  The following node will then infect any of its neighbours that receive an edge after the
  time it was first infected. This repeats until the latest timestamp/iterations are met.

Parameters
  startTime Long : Time to start spreading taint
  infectedNodes Set[Long] : List of nodes that will start as tainted
  stopNodes Set[Long] : If set, any nodes that will not propogate taint
  output String : Path where the output will be written

Returns
  Infected ID (Long) : ID of the infected vertex
  Edge ID (Long) : Edge that propogated the infection
  Time (Long) : Time of infection
  Infector ID (Long) : Node that spread the infection
**/
class GenericTaint(startTime: Long, infectedNodes: Set[Long], stopNodes: Set[Long] = Set(), output: String = "/tmp/generic_taint") extends GraphAlgorithm{

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph
      // the step functions run on every single vertex ONCE at the beginning of the algorithm
      .step({
        // for each vertex in the graph
        vertex =>
          // check if it is one of our infected nodes
          if (infectedNodes contains vertex.ID) {
            // set its state to tainted
            val result = List(("tainted", -1, startTime, "startPoint"))
            // set this node as the beginning state
            vertex.setState("taintStatus", true)
            vertex.setState("taintHistory", result)
            // tell all the other nodes that it interacts with, after starTime, that they are also infected
            // but also send the properties of when the infection happened
            vertex.getOutEdges(after = startTime).foreach(
              // for every edge/transaction this node made after the time
              edge =>
                // get all the transactions that this node sent
                edge.history().foreach(
                  event =>
                    if (event.event & event.time >= startTime) {
                      // if it was a sent transaction and the time is after the start, mark as an edge
                      edge.send(Tuple4("tainted",
                        edge.ID(),
                        event.time,
                        vertex.ID()
                      )
                      )
                    }
                )
            )
          }
      })
      .iterate({
        vertex =>
          // check if any node has received a message
          // obtain the messages as a set
          val newMessages = vertex.messageQueue[(String, Long, Long, Long)].map(item => item).distinct
          // obtain the min time it was infected
          val infectionTime = newMessages.map(item => item._3).min
          val status = newMessages.map(item => item._1).distinct
          // check if any of the newMessages are the keyword tainted
          if (status contains "tainted") {
            // check if it was previous tainted
            // if not set the new state, which contains the sources of taint
            if (vertex.getOrSetState("taintStatus", false) == false) {
              vertex.setState("taintHistory", newMessages)
              vertex.setState("taintStatus", true)
            }
            else {
              // otherwise set a brand new state, first get the old txs it was tainted by
              val oldState: List[(String, Long, Long, Long)] = vertex.getState("taintHistory")
              // add the new transactions and old ones together
              val newState = List.concat(newMessages, oldState).distinct
              // set this as the new state
              vertex.setState("taintHistory", newState)
            }
            // if we have no stop nodes set, then continue the taint spreading
            if (stopNodes.size == 0) {
              // repeat like we did in the step
              vertex.getOutEdges(after = infectionTime).foreach(
                edge => edge.history().foreach(
                  event => if (event.event & event.time >= infectionTime) {
                    edge.send(Tuple4("tainted",
                      edge.ID(),
                      event.time,
                      vertex.ID()
                    )
                    )
                  }
                )
              )
            }
            else if (!(stopNodes contains vertex.ID)) {
              // otherwise if we contain stop nodes and we dont have to stop then keep going
              // repeat like we did in the step
              vertex.getOutEdges(after = infectionTime).foreach(
                edge => edge.history().foreach(
                  event => if (event.event & event.time >= infectionTime) {
                    edge.send(Tuple4("tainted",
                      edge.ID(),
                      event.time,
                      vertex.ID()
                    )
                    )
                  }
                )
              )
            }
          }
          else
            vertex.voteToHalt()
      }, 100, true)
    // get all vertexes and their status
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select(vertex => Row(
      vertex.ID(),
      vertex.getStateOrElse("taintStatus", false),
      vertex.getStateOrElse[Any]("taintHistory", "false")
    ))
      // filter for any that had been tainted and save to folder
      .filter(r => r.get(1) == true)
      .explode(row => row.get(2).asInstanceOf[List[(String, Long, Long, Long)]].map(
        tx => Row(row(0), tx._2, tx._3, tx._4)
      ))
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object GenericTaint {
  def apply(startTime: Int, infectedNodes: Set[Long], stopNodes: Set[Long] = Set(), output: String) =
    new GenericTaint(startTime, infectedNodes, stopNodes, output)
}