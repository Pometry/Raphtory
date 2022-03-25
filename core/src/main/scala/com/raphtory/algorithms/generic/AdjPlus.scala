package com.raphtory.algorithms.generic

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

/**
  * {s}`AdjPlus()`
  * : AdjPlus transform of the graph
  *
  *  This transforms the graph using the AdjPlus projection, which treats the input graph as undirected and returns a
  *  directed graph where all edges point from low to high degree.
  *  For each vertex, the algorithm finds the set of neighbours that have a larger degree than the current vertex
  *  or the same degree and a larger ID and store it as state "adjPlus". This algorithm treats the network as undirected.
  *  Further, the vertex IDs in "adjPlus" are ordered by increasing degree. This projection is particularly useful to
  *  make certain motif-counting algorithms more efficient.
  *
  * ## States
  *
  *  {s}`adjPlus: Array[Long]`
  *  : List of neighbour IDs that have a larger degree than the current vertex
  *    or the same degree and a larger ID, ordered by increasing degree
  *
  * ## Returns
  *  edge list for the AdjPlus projection
  *
  *  | source name          | destination name     |
  *  | -------------------- | -------------------- |
  *  | {s}`srcName: String` | {s}`dstName: String` |
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.generic.motif.SquareCount)
  * ```
  */
class AdjPlus extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph.step(vertex => vertex.messageAllNeighbours((vertex.ID(), vertex.degree))).step { vertex =>
      val degree = vertex.degree
      //        Find set of neighbours with higher degree
      val adj    = vertex
        .messageQueue[(Long, Int)]
        .filter(message =>
          degree < message._2 || (message._2 == degree && vertex.ID() < message._1)
        )
        .sortBy(m => (m._2, m._1))
        .map(message => message._1)
        .toArray
      vertex.setState("adjPlus", adj)
    }

  override def tabularise(graph: GraphPerspective): Table =
//    return adjPlus as edge list
    graph
      .step { vertex =>
        val adj = vertex.getState[Array[Long]]("adjPlus")
        adj.foreach(a => vertex.messageVertex(a, vertex.ID()))
      }
      .step(vertex =>
        vertex.messageQueue[Long].foreach(v => vertex.messageVertex(v, vertex.name()))
      )
      .select(vertex => Row(vertex.name(), vertex.messageQueue[String]))
      .explode(row => row.getAs[List[String]](1).map(v => Row(row.get(0), v)))
}

object AdjPlus {
  def apply() = new AdjPlus()
}
