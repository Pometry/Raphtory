package com.raphtory.algorithms.newer

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}
import scala.util.Random

class WattsCascade(infectedSeed:Array[Long], UNIFORM_RANDOM:Int = 1, UNIFORM_SAMEVAL:Int = 2, threshold_choice:Double = 1.0, seed:Int = -1, output:String = "/tmp/wattsCascade") extends GraphAlgorithm {

  val randomiser = if (seed != -1) new Random(seed) else new Random()

  override def algorithm(graph: GraphPerspective): Unit = {
    graph
      .step({
        vertex =>
          if (infectedSeed.contains(vertex.ID())) {
            vertex.setState("infected",true)
            vertex.messageAllNeighbours(1.0)
          }
          else {
            vertex.setState("infected",false)
          }
          if (threshold_choice == UNIFORM_RANDOM) {
            vertex.setState("threshold", randomiser.nextFloat())
          } else {
            vertex.setState("threshold",threshold_choice)
          }
      })
      .iterate({
        vertex =>
          val degree = vertex.getInEdges().size + vertex.getOutEdges().size
          val newLabel =
            if (degree > 0 && !vertex.getStateOrElse[Boolean]("infected", false))
              (vertex.messageQueue[Double].sum/degree > threshold_choice)
            else
              vertex.getState[Boolean]("infected")
          if (newLabel != vertex.getState[Boolean]("infected")) {
            vertex.messageAllNeighbours(1.0)
            vertex.setState("infected",true)
          } else
            vertex.voteToHalt()
      }, 100, true)
      .select({ vertex => Row(
        vertex.ID,
        vertex.getPropertyOrElse[String]("name", "None"),
        vertex.getStateOrElse[Boolean]("infected", false))}
      )
      .writeTo(output)
  }
}

object WattsCascade{
  def apply(infectedSeed:Array[Long], UNIFORM_RANDOM:Int = 1, UNIFORM_SAMEVAL:Int = 2, threshold_choice:Double = 1.0, seed:Int = -1, output:String = "/tmp/wattsCascade") =
    new WattsCascade(infectedSeed, UNIFORM_RANDOM, UNIFORM_SAMEVAL, threshold_choice, seed, output)
}