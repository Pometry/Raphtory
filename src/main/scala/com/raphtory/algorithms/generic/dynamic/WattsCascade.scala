package com.raphtory.algorithms.generic.dynamic

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

import scala.util.Random

/**
Description
  This algorithm will run the Watts Cascade. This algorithm, presented by Duncan Watts
  in 2002, presents a method for the spread of "infectious ideas." In the model, people
  are represented as nodes and relationships are edges. Each node is given a random or
  deterministic threshold that states it will accept an idea if the threshold of its
  neighbours accepting the idea has exceeded.
  1. In the first step the state of all nodes are set. This includes whether they are
     initially infected and their threshold.

  2. Each non-infected vertex checks whether the number of infected messages it has
     received outweighs its threshold, if so then it sets its state to be infected and
     then annouces this to all of its neighbours.

Parameters
  infectedSeed (Array(Long)) : The list of node IDs to that begin infection.
  UNIFORM_RANDOM (Int) : If equal to threshold choice, then all thresholds will be set at random.
  threshold_choice (Double) : Default threshold to trigger change of state.
  seed (Int) : Value used for the random selection, can be set to ensure same result is returned
                per run. If not specified, it will generate a random seed.
  output (String) : The path where the output will be saved. If not specified,
                    defaults to /tmp/wattsCascade.

Returns
  Node ID (Long): Vertex ID
  Name of vertex(String): name property of vertex if set
  Infected (Boolean): True or False, whether node was infected or not
**/
class WattsCascade(infectedSeed:Array[Long], UNIFORM_RANDOM:Int = 1, UNIFORM_SAMEVAL:Int = 2, threshold_choice:Double = 1.0, seed:Int = -1, output:String = "/tmp/wattsCascade") extends GraphAlgorithm {

  val randomiser = if (seed != -1) new Random(seed) else new Random()

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph
      .step({
        vertex =>
          if (infectedSeed.contains(vertex.ID())) {
            vertex.setState("infected", true)
            vertex.messageAllNeighbours(1.0)
          }
          else {
            vertex.setState("infected", false)
          }
          if (threshold_choice == UNIFORM_RANDOM) {
            vertex.setState("threshold", randomiser.nextFloat())
          } else {
            vertex.setState("threshold", threshold_choice)
          }
      })
      .iterate({
        vertex =>
          val degree = vertex.getInEdges().size + vertex.getOutEdges().size
          val newLabel =
            if (degree > 0 && !vertex.getStateOrElse[Boolean]("infected", false))
              (vertex.messageQueue[Double].sum / degree > threshold_choice)
            else
              vertex.getState[Boolean]("infected")
          if (newLabel != vertex.getState[Boolean]("infected")) {
            vertex.messageAllNeighbours(1.0)
            vertex.setState("infected", true)
          } else
            vertex.voteToHalt()
      }, 100, true)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({ vertex =>
      Row(
        vertex.name(),
        vertex.getPropertyOrElse[String]("name", "None"),
        vertex.getStateOrElse[Boolean]("infected", false))
    }
    )
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
}

object WattsCascade{
  def apply(infectedSeed:Array[Long], UNIFORM_RANDOM:Int = 1, UNIFORM_SAMEVAL:Int = 2, threshold_choice:Double = 1.0, seed:Int = -1, output:String = "/tmp/wattsCascade") =
    new WattsCascade(infectedSeed, UNIFORM_RANDOM, UNIFORM_SAMEVAL, threshold_choice, seed, output)
}