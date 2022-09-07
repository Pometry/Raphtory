package com.raphtory.algorithms.generic.dynamic

import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.utils.Sampling._

import scala.math.Numeric.Implicits._

class WeightedRandomWalk[T: Numeric](
    walkLength: Int,
    numWalks: Int,
    seed: Long = -1,
    weight: String = "weight"
) extends RandomWalk(walkLength, numWalks, seed) {

  override protected def selectNeighbour(vertex: Vertex): vertex.IDType = {
    val neighbours = vertex.outNeighbours
    if (neighbours.isEmpty)
      vertex.ID
    else {
      val weights = vertex.outEdges.map(e => e.weight[T](weight).toDouble).toVector
      val i = rnd.sample(weights)
      neighbours.zipWithIndex.find{case (_, j) => i == j}.get._1
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
