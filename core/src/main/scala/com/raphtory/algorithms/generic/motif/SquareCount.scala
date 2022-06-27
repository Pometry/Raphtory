package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.generic.AdjPlus
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

import scala.collection.mutable

case class FirstStep[VertexID](p: VertexID, adj: Array[VertexID])
case class SecondStep[VertexID](p: VertexID, q: VertexID, adj: Array[VertexID])
case class CountMessage(count: Long)
case class WedgeMessage[VertexID](p: VertexID, s: Array[VertexID])

object AccumulateCounts extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        //        count pr and qr squares and forward counts for accumulating
        var count = vertex.getStateOrElse[Long]("squareCount", 0L)
        val adj   = vertex.getState[Array[vertex.IDType]]("adjPlus").toSet
        vertex.messageQueue[SecondStep[vertex.IDType]].foreach { message =>
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

object CountPR extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = {
    val counts = AccumulateCounts
    AccumulateCounts(
            graph
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IDType]]("adjPlus")
                //        message neighbour set for pr and qr counting
                adj.foreach(neighbour_id => vertex.messageVertex(neighbour_id, FirstStep(vertex.ID, adj)))
              }
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IDType]]("adjPlus")
                vertex.messageQueue[FirstStep[vertex.IDType]].foreach { message =>
                  val p     = message.p
                  val adj_p = message.adj
                  //          forward neighbour set for pr square counting
                  adj.foreach(neighbour_id => vertex.messageVertex(neighbour_id, SecondStep(p, vertex.ID, adj_p)))
                }
              }
    )
  }
}

object CountQR extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    AccumulateCounts(
            graph
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IDType]]("adjPlus")
                //        message neighbour set for pr and qr counting
                for (i <- adj.indices.dropRight(1))
                  vertex.messageVertex(adj(i), FirstStep(vertex.ID, adj.slice(i + 1, adj.length)))
              }
              .step { vertex =>
                val adj = vertex.getState[Array[vertex.IDType]]("adjPlus")
                vertex.messageQueue[FirstStep[vertex.IDType]].foreach { message =>
                  val p     = message.p
                  val adj_p = message.adj
                  //          forward neighbour set for pr square counting
                  adj_p.foreach(neighbour_id => vertex.messageVertex(neighbour_id, SecondStep(p, vertex.ID, adj)))
                }
              }
    )
}

object CountPQ extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        //        pq wedge count messages (wedges are counted on lower-degree wedge vertex)
        val adj = vertex.getState[Array[vertex.IDType]]("adjPlus")
        for (i <- adj.indices.dropRight(1)) {
          val r = adj(i)
          vertex.messageVertex(r, WedgeMessage(vertex.ID, adj.slice(i + 1, adj.length)))
        }
      }
      .step { vertex =>
        //        count wedges
        var count      = vertex.getStateOrElse[Long]("squareCount", 0L)
        val wedgeCount = mutable.Map[vertex.IDType, mutable.ArrayBuffer[vertex.IDType]]()
        vertex.messageQueue[WedgeMessage[vertex.IDType]].foreach { message =>
          message.s.foreach(s => wedgeCount.getOrElseUpdate(s, mutable.ArrayBuffer[vertex.IDType]()).append(message.p))
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
object SquareCount extends NodeList(Seq("squareCount")) {

  override def apply(graph: GraphPerspective): graph.Graph =
    CountPQ(CountQR(CountPR(AdjPlus(graph))))
}
