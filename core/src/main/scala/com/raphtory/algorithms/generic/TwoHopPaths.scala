package com.raphtory.algorithms.generic

import com.raphtory.algorithms.generic.TwoHopPaths.Message
import com.raphtory.algorithms.generic.TwoHopPaths.RequestFirstHop
import com.raphtory.algorithms.generic.TwoHopPaths.RequestSecondHop
import com.raphtory.algorithms.generic.TwoHopPaths.Response
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.collection.mutable.ArrayBuffer

/**
  * {s}`TwoHopPaths()`
  *    : List all two-hop paths in the network
  *
  * {s}`TwoHopPaths(seeds: Iterable[String])`, {s}`TwoHopPaths(seeds: String*)`
  *    : List all two-hop paths starting at nodes in {s}`seeds`
  *
  * This algorithm will return the two hop neighbours of each node in
  * the graph. If the user provides input seeds, then it will only return
  * the two hop neighbours starting from those nodes.
  *
  * ```{Warning}
  *    As this sends alot of messages between nodes, running this for the entire
  *    graph with a large number of nodes may cause you to run out of memory.
  *    Therefore it is most optimal to run with a few select nodes at a time.
  * ```
  *
  * ## Parameters
  *
  *  {s}`seeds: Set[String]`
  *    : The set of node names to start paths from. If not specified, then this is
  *      run for all nodes.
  *
  * ## Returns
  *
  *  | vertex1           | vertex2           | vertex3           |
  *  | ----------------- | ----------------- | ----------------- |
  *  | {s}`name: String` | {s}`name: String` | {s}`name: String` |
  *
  * ## Implementation
  *
  *  1. In the first step the node messages all its neighbours, saying that it is
  *     asking for a two-hop analysis.
  *
  *  2. The first-hop node forwards the message to all its neighbours, adding its name
  *
  *  3. The second-hop node adds its name and responds to the source
  *
  *  4. The source node compiles all response messages
  */
class TwoHopPaths(seeds: Set[String] = Set[String]()) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step(vertex =>
        if (seeds.isEmpty || seeds.contains(vertex.name()))
          vertex.messageOutNeighbours(RequestFirstHop(vertex.ID))
      )
      .iterate(
              { vertex =>
                val newMessages = vertex.messageQueue[Message[vertex.IDType]]
                newMessages.foreach {
                  message =>
                    message match {
                      case RequestFirstHop(source)            =>
                        vertex
                          .getOutNeighbours()
                          .foreach(n =>
                            if (n != source)
                              vertex.messageVertex(n, RequestSecondHop(source, vertex.name()))
                          )
                      case RequestSecondHop(source, firstHop) =>
                        vertex.messageVertex(source, Response(firstHop, vertex.name()))
                      case Response(firstHop, secondHop)      =>
                        val paths = vertex.getOrSetState[ArrayBuffer[Array[String]]](
                                "twoHopPaths",
                                ArrayBuffer[Array[String]]()
                        )
                        paths.append(Array[String](firstHop, secondHop))
                    }
                }
              },
              3,
              true
      )

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .select(vertex =>
        Row(
                vertex.name(),
                vertex.getStateOrElse("twoHopPaths", ArrayBuffer[Array[String]]())
        )
      )
      .explode(row =>
        row
          .getAs[ArrayBuffer[Array[String]]](1)
          .toList
          .map(hops => Row(row.get(0), hops(0), hops(1)))
      )
}

object TwoHopPaths {
  def apply(seeds: Iterable[String] = Set[String]()) = new TwoHopPaths(seeds.toSet)
  def apply(seeds: String*)                          = new TwoHopPaths(seeds.toSet)

  sealed trait Message[VertexID]
  case class RequestFirstHop[VertexID](source: VertexID) extends Message[VertexID]

  case class RequestSecondHop[VertexID](source: VertexID, firstHop: String) extends Message[VertexID]
  case class Response[VertexID](firstHop: String, secondHop: String)        extends Message[VertexID]
}
