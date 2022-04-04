package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.api.Chain
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.algorithms.generic.AdjPlus
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.graph.visitor.Vertex

import scala.collection.mutable

case class FirstStep[VertexID](p: VertexID, adj: Array[VertexID])
case class SecondStep[VertexID](p: VertexID, q: VertexID, adj: Array[VertexID])
case class CountMessage(count: Long)
case class WedgeMessage[VertexID](p: VertexID, s: Array[VertexID])

class AccumulateCounts() extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        //        count pr and qr squares and forward counts for accumulating
        var count = vertex.getStateOrElse[Long]("squareCount", 0L)
        val adj   = vertex.getState[Array[vertex.IdType]]("adjPlus").toSet
        vertex.messageQueue[SecondStep[vertex.IdType]].foreach { message =>
          val adj_n      = message.adj
          val p          = message.p
          val q          = message.q
          val squareS    = adj.intersect(adj_n.toSet)
          val localCount = squareS.size
          vertex.messageVertex(p, CountMessage(localCount))
          vertex.messageVertex(q, CountMessage(localCount))
          count += localCount
          squareS.foreach(s => vertex.messageVertex(s, CountMessage(1)))
        }
        vertex.setState("squareCount", count)
      }
      .step { vertex =>
        //        accumulate pr and qr counts identified at different vertices
        var count = vertex.getStateOrElse[Long]("squareCount", 0L)
        vertex.messageQueue[CountMessage].foreach(message => count += message.count)
        vertex.setState("squareCount", count)
      }
}

object AccumulateCounts {
  def apply() = new AccumulateCounts()
}

class CountPR() extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    AccumulateCounts()(
            graph
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IdType]]("adjPlus")
                //        message neighbour set for pr and qr counting
                adj.foreach(neighbour_id =>
                  vertex.messageVertex(neighbour_id, FirstStep(vertex.ID(), adj))
                )
              }
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IdType]]("adjPlus")
                vertex.messageQueue[FirstStep[vertex.IdType]].foreach { message =>
                  val p     = message.p
                  val adj_p = message.adj
                  //          forward neighbour set for pr square counting
                  adj.foreach(neighbour_id =>
                    vertex.messageVertex(neighbour_id, SecondStep(p, vertex.ID(), adj_p))
                  )
                }
              }
    )
}

object CountPR {
  def apply() = new CountPR()
}

class CountQR() extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    AccumulateCounts()(
            graph
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IdType]]("adjPlus")
                //        message neighbour set for pr and qr counting
                for (i <- adj.indices.dropRight(1))
                  vertex.messageVertex(adj(i), FirstStep(vertex.ID(), adj.slice(i + 1, adj.length)))
              }
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IdType]]("adjPlus")
                vertex.messageQueue[FirstStep[vertex.IdType]].foreach { message =>
                  val p     = message.p
                  val adj_p = message.adj
                  //          forward neighbour set for pr square counting
                  adj_p.foreach(neighbour_id =>
                    vertex.messageVertex(neighbour_id, SecondStep(p, vertex.ID(), adj))
                  )
                }
              }
    )
}

object CountQR {
  def apply() = new CountQR()
}

class CountPQ() extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        //        pq wedge count messages (wedges are counted on lower-degree wedge vertex)
        val adj = vertex.getState[Array[vertex.IdType]]("adjPlus")
        for (i <- adj.indices.dropRight(1)) {
          val r = adj(i)
          vertex.messageVertex(r, WedgeMessage(vertex.ID(), adj.slice(i + 1, adj.length)))
        }
      }
      .step { vertex =>
        //        count wedges
        var count      = vertex.getStateOrElse[Long]("squareCount", 0L)
        val wedgeCount = mutable.Map[vertex.IdType, mutable.ArrayBuffer[vertex.IdType]]()
        vertex.messageQueue[WedgeMessage[vertex.IdType]].foreach { message =>
          message.s.foreach(s =>
            wedgeCount.getOrElseUpdate(s, mutable.ArrayBuffer[vertex.IdType]()).append(message.p)
          )
        }
        wedgeCount.foreach({
          case (s, others) =>
            val numSquares = others.size * (others.size - 1) / 2
            count += numSquares
            vertex.messageVertex(s, CountMessage(numSquares))
            others.foreach(p => vertex.messageVertex(p, CountMessage(numSquares)))
        })
        vertex.setState("squareCount", count)
      }
      .step { vertex =>
        //        accumulate pq counts
        var count = vertex.getState[Long]("squareCount")
        vertex.messageQueue[CountMessage].foreach(message => count += message.count)
        vertex.setState("squareCount", count)
      }
}

object CountPQ {
  def apply() = new CountPQ()
}

/**
  * {s}`SquareCount()`
  *   : Count undirected squares that a vertex is part of
  *
  *  This is similar to counting triangles and especially useful for
  *  bipartite graphs. This implementation is based on the algorithm from
  *  [Towards Distributed Square Counting in Large Graphs](https://doi.org/10.1109/hpec49654.2021.9622799)
  *
  * ## States
  *
  *  {s}`adjPlus: Array[Long]`
  *    : List of neighbours that have a larger degree than the current vertex or the same degree and a larger ID
  *      as computed by the [](com.raphtory.algorithms.generic.AdjPlus) algorithm.
  *
  *  {s}`squareCount: Long`
  *    : Number of squares the vertex is part of
  *
  * Returns
  *
  *  | vertex name       | square count           |
  *  | ----------------- | ---------------------- |
  *  | {s}`name: String` | {s}`squareCount: Long` |
  */
class SquareCount()
        extends Chain(Seq(AdjPlus(), CountPR(), CountQR(), CountPQ(), NodeList("squareCount")))

object SquareCount {
  def apply() = new SquareCount()
}
