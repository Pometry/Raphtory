package com.raphtory.algorithms.generic.dynamic

import com.raphtory.graph.visitor.Vertex
import com.raphtory.util.Sampling._

class WeightedRandomWalk[T](
    walkLength: Int,
    numWalks: Int,
    seed: Long = -1,
    weight: String = "weight"
)(implicit numeric: Numeric[T])
        extends RandomWalk(walkLength, numWalks, seed) {

  override protected def selectNeighbour(vertex: Vertex): vertex.IdType = {
    val neighbours = vertex.getOutNeighbours()
    if (neighbours.isEmpty)
      vertex.ID()
    else {
      val weights = vertex.getOutEdges().map { edge =>
        numeric.toDouble(
                edge
                  .explode()
                  .map(explodedEdge => explodedEdge.getProperty[T](weight).getOrElse(numeric.one))
                  .sum
        )
      }
      neighbours(rnd.sample(weights))
    }
  }
}

object WeightedRandomWalk {

  def apply[T: Numeric](
      walkLength: Int = 10,
      numWalks: Int = 1,
      seed: Long = -1,
      weight: String = "weight"
  ) =
    new WeightedRandomWalk[T](walkLength, numWalks, seed, weight)
}
