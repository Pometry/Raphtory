package com.raphtory.algorithms.generic.dynamic

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.util.Sampling._

import scala.math.Numeric.Implicits._

class WeightedRandomWalk[T: Numeric](
    walkLength: Int,
    numWalks: Int,
    seed: Long = -1,
    weight: String = "weight"
) extends RandomWalk(walkLength, numWalks, seed) {

  override protected def selectNeighbour(vertex: Vertex): vertex.IDType = {
    val neighbours = vertex.getOutNeighbours()
    if (neighbours.isEmpty)
      vertex.ID
    else {
      val weights = vertex.getOutEdges().map(e => e.weight[T](weight).toDouble)
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
