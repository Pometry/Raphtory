package com.raphtory.algorithms.generic

import collection.mutable.ArrayBuffer
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Table, Row}
import com.raphtory.core.model.graph.visitor.Vertex


/**
 * Count occurrences of three-node motifs that a node participates in.
 *
 * The motifs are:
 *
 * (stars)
 * 0: l <- c -> r
 * 1: l -> c <- r
 * 2: l -> c -> r
 * 3: l -> c <-> r
 * 4: l <- c <-> r
 * 5: l <-> c <-> r
 *
 * (triangles) Index of star + third edge
 * 6: 0 + l -> r
 * 7: 2 + l <- r
 * 8: 3 + l -> r
 * 9: 4 + l -> r
 * 10: 4 + l <- r
 * 11: 5 + l -> r
 * 12: 5 + l <-> r
 *
 * The algorithm works by first counting star motifs, including potential triangles.
 * Then, for each star we send a message to one of the neighbours to identify the potential third edge.
 * Based on the responses, we correct the star counts and count the triangles.
 *
 * Note that stars are only counted once as they only appear in the counts for the central vertex. However, triangles
 * appear for each of the 3 vertices, so the count for triangles should be divided by 3 when aggregating results to get
 * motif counts for the whole graph.
 *
 * @param output: output path
 *
 * Motif counts are stored as an array (see indices above) with key "motifCounts"
 *
 * Columns in tabularised results:
 * timestamp, vertexID, vertexName, count0, ... , count12
 *
 */
class ThreeNodeMotifs(output: String = "/tmp/ThreeNodeMotifs") extends GraphAlgorithm {
//  Mapping of edge type of left and right edge to star motif (negative values indicate flipped motif)
  val starMotifIndex = Map((1, 1) -> 0, (2, 2) -> 1, (2, 1) -> 2, (1, 2) -> -2, (2, 0) -> 3, (0, 2) -> -3, (1, 0) -> 4,
    (0, 1) -> -4, (0, 0) -> 5)

//  Mapping of star motif index and edge type of third edge to triangle motif index
  val triangleMotifIndex = Map((0, 0) -> 8, (0, 1) -> 6, (0, 2) -> 6, (1, 0) -> 10, (1, 1) -> 6, (1, 2) -> 6,
    (2, 0) -> 9, (-2, 0) -> 9, (2, 1) -> 6, (-2, 1) -> 7, (2, 2) -> 7, (-2, 2) -> 6, (3, 0) -> 11, (-3, 0) -> 11,
    (3, 1) -> 8, (-3, 1) -> 9, (3, 2) -> 9, (-3, 2) -> 8, (4, 0) -> 11, (-4, 0) -> 11, (4, 1) -> 9, (-4, 1) -> 10,
    (4, 2) -> 10, (-4, 2) -> 9, (5, 0) -> 12, (5, 1) -> 11, (5, 2) -> 11)

  def _getEdgeType(vertex: Vertex): Long => Int = {
    val outNeighbours = vertex.getOutNeighbours().toSet
    val inNeighbours = vertex.getInNeighbours().toSet
    vID => {
    if (outNeighbours.contains(vID) && inNeighbours.contains(vID)) {
      0
    } else if (outNeighbours.contains(vID)) {
      1
    } else if (inNeighbours.contains(vID)) {
      2
    } else {
      -1
    }
    }
  }

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step(vertex => {
      val motifCounts = ArrayBuffer.fill[Long](13)(0)

      val neighbours = vertex.getAllNeighbours().toArray
      val getEdgeType: Long => Int = _getEdgeType(vertex)

      for (i <- neighbours.indices) {
        val first = neighbours(i)
        for (j <- i+1 until neighbours.length) {
          val second = neighbours(j)
          val mType = starMotifIndex((getEdgeType(first), getEdgeType(second)))
          motifCounts(mType.abs) += 1
          vertex.messageVertex(first, (vertex.ID(), second, mType))
        }
      }
      vertex.setState("motifCounts", motifCounts)
    })
      .step(vertex => {
        val messages = vertex.messageQueue[(Long, Long, Int)]
        val getEdgeType: Long => Int = _getEdgeType(vertex)

        for ((source, second, mType) <- messages) {
          val eType = getEdgeType(second)
          if (eType >= 0) {
//            if connected, message source back with edge type
            vertex.messageVertex(source, (mType, eType))
          }
        }
      })
      .step(vertex => {
        val messages = vertex.messageQueue[(Int, Int)]
        val motifCounts = vertex.getState[ArrayBuffer[Long]]("motifCounts")

        for ((mType, eType) <- messages) {
          motifCounts(mType.abs) -= 1
          motifCounts(triangleMotifIndex(mType, eType)) += 1
        }
        vertex.setState("motifCounts", motifCounts)
      })
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select(vertex => {
      val motifCounts = vertex.getState[ArrayBuffer[Long]]("motifCounts")
      val row = vertex.ID() +: vertex.getPropertyOrElse("name", "unknown") +: motifCounts

      Row(row: _*)
    })
  }

  override def write(table: Table): Unit = table.writeTo(output)
}

object ThreeNodeMotifs {
  def apply(output: String = "/tmp/ThreeNodeMotifs") = new ThreeNodeMotifs(output)
}
