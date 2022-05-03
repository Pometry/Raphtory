package com.raphtory.algorithms.generic.motif

import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table
import com.raphtory.graph.visitor.Vertex

import scala.collection.mutable.ArrayBuffer

/**
  *  `ThreeNodeMotifs()`
  *    : Count occurrences of three-node motifs that a node participates in.
  *
  *  The algorithm works by first counting star motifs, including potential triangles.
  *  Then, for each star we send a message to one of the neighbours to identify the potential third edge.
  *  Based on the responses, we correct the star counts and count the triangles.
  *
  *  ```{note}
  *  Stars are only counted once as they only appear in the counts for the central vertex. However, triangles
  *  appear for each of the 3 vertices, so the count for triangles should be divided by 3 when aggregating results to get
  *  motif counts for the whole graph.
  *  ```
  *
  *  ## Motifs
  *
  *  ### Stars
  *
  *    - 0: l <- c -> r
  *    - 1: l -> c <- r
  *    - 2: l -> c -> r
  *    - 3: l -> c <-> r
  *    - 4: l <- c <-> r
  *    - 5: l <-> c <-> r
  *
  *  ### Triangles
  *
  *  (Index of star + third edge)
  *    - 6: 0 + l -> r
  *    - 7: 2 + l <- r
  *    - 8: 3 + l -> r
  *    - 9: 4 + l -> r
  *    - 10: 4 + l <- r
  *    - 11: 5 + l -> r
  *    - 12: 5 + l <-> r
  *
  * ## States
  *  `motifCounts: Array[Long]`
  *    : Motif counts stored as an array (see indices above)
  *
  * ## Returns
  *
  *  | vertex name       | Motif 0                   | ... | Motif 12                   |
  *  | ----------------- | ------------------------- | --- | -------------------------- |
  *  | `name: String` | `motifCounts(0): Long` | ... | `motifCounts(12): Long` |
  */
class ThreeNodeMotifs() extends GraphAlgorithm {
  //  edge direction constants
  val inoutEdge = 0
  val outEdge   = 1
  val inEdge    = 2
  val noEdge    = -1

  //  Mapping of edge type of left and right edge to star motif (negative values indicate flipped motif)
  val starMotifIndex = Map(
          (outEdge, outEdge)     -> 0,
          (inEdge, inEdge)       -> 1,
          (inEdge, outEdge)      -> 2,
          (outEdge, inEdge)      -> -2,
          (inEdge, inoutEdge)    -> 3,
          (inoutEdge, inEdge)    -> -3,
          (outEdge, inoutEdge)   -> 4,
          (inoutEdge, outEdge)   -> -4,
          (inoutEdge, inoutEdge) -> 5
  )

  //  Mapping of star motif index and edge type of third edge to triangle motif index
  val triangleMotifIndex = Map(
          (0, inoutEdge)  -> 8,
          (0, outEdge)    -> 6,
          (0, inEdge)     -> 6,
          (1, inoutEdge)  -> 10,
          (1, outEdge)    -> 6,
          (1, inEdge)     -> 6,
          (2, inoutEdge)  -> 9,
          (-2, inoutEdge) -> 9,
          (2, outEdge)    -> 6,
          (-2, outEdge)   -> 7,
          (2, inEdge)     -> 7,
          (-2, inEdge)    -> 6,
          (3, inoutEdge)  -> 11,
          (-3, inoutEdge) -> 11,
          (3, outEdge)    -> 8,
          (-3, outEdge)   -> 9,
          (3, inEdge)     -> 9,
          (-3, inEdge)    -> 8,
          (4, inoutEdge)  -> 11,
          (-4, inoutEdge) -> 11,
          (4, outEdge)    -> 9,
          (-4, outEdge)   -> 10,
          (4, inEdge)     -> 10,
          (-4, inEdge)    -> 9,
          (5, inoutEdge)  -> 12,
          (5, outEdge)    -> 11,
          (5, inEdge)     -> 11
  )

  def _getEdgeType(vertex: Vertex): vertex.IDType => Int = { vID =>
    if (vertex.isInNeighbour(vID) && vertex.isOutNeighbour(vID))
      inoutEdge
    else if (vertex.isOutNeighbour(vID))
      outEdge
    else if (vertex.isInNeighbour(vID))
      inEdge
    else
      noEdge
  }

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        import vertex._
        val motifCounts                       = ArrayBuffer.fill[Long](13)(0)
        val neighbours                        = vertex.getAllNeighbours().toArray
        val getEdgeType: vertex.IDType => Int = _getEdgeType(vertex)

        for (i <- neighbours.indices) {
          val first = neighbours(i)
          for (j <- i + 1 until neighbours.length) {
            val second = neighbours(j)
            val mType  = starMotifIndex((getEdgeType(first), getEdgeType(second)))
            motifCounts(mType.abs) += 1
            vertex.messageVertex(first, (vertex.ID(), second, mType))
          }
        }
        vertex.setState("motifCounts", motifCounts)
      }
      .step { vertex =>
        val messages                          = vertex.messageQueue[(vertex.IDType, vertex.IDType, Int)]
        val getEdgeType: vertex.IDType => Int = _getEdgeType(vertex)

        for ((source, second, mType) <- messages) {
          val eType = getEdgeType(second)
          if (eType != noEdge)
//            if connected, message source back with edge type
            vertex.messageVertex(source, (mType, eType))
        }
      }
      .step { vertex =>
        val messages    = vertex.messageQueue[(Int, Int)]
        val motifCounts = vertex.getState[ArrayBuffer[Long]]("motifCounts")

        for ((mType, eType) <- messages) {
          motifCounts(mType.abs) -= 1
          motifCounts(triangleMotifIndex(mType, eType)) += 1
        }
        vertex.setState("motifCounts", motifCounts)
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.select { vertex =>
      val motifCounts = vertex.getState[ArrayBuffer[Long]]("motifCounts")
      val row         = vertex.name() +: motifCounts
      Row(row.toSeq: _*)
    }

}

object ThreeNodeMotifs {
  def apply() = new ThreeNodeMotifs()
}
