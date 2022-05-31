package com.raphtory.algorithms.generic.dynamic

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.generic.dynamic.WattsCascade.Threshold
import com.raphtory.algorithms.api.GraphPerspective

import scala.util.Random

/**
  * {s}`WattsCascade(infectedSeed:Iterable[String], threshold: T = Threshold.UNIFORM_RANDOM, seed:Long = -1, maxGenerations: Int = 100)`
  *  : run the Watts Cascade dynamic on the network
  *
  *  This algorithm, presented by Duncan Watts in 2002, presents a method for the spread of "infectious ideas."
  *  In the model, people are represented as nodes and relationships are edges. Each node is given a random or
  *  deterministic threshold that states it will accept an idea if the fraction its in-neighbours accepting the
  *  idea has exceeded the threshold.
  *
  *  1. In the first step the state of all nodes are set. This includes whether they are
  *     initially infected and their threshold.
  *
  *  2. Each non-infected vertex checks whether the number of infected messages it has
  *     received outweighs its threshold, if so then it sets its state to be infected and
  *     then announces this to all of its out-neighbours.
  *
  * ## Parameters
  *
  *  {s}`infectedSeed: Seq[String]`
  *    : The list of node names that begin infection.
  *
  *  {s}`threshold: Double | Threshold.UNIFORM_RANDOM | Threshold.RANDOM_SAME_VAL = Threshold.UNIFORM_RANDOM`
  *    : fraction of infected neighbours necessary to trigger change of state. Set to {s}`UNIFORM_RANDOM` to choose
  *      thresholds independently at random for each vertex and {s}`RANDOM_SAME_VAL` to choose a single random threshold
  *      to apply to all vertices..
  *
  *  {s}`seed: Long`
  *    : Value used for the random selection, can be set to ensure same result is returned per run.
  *      If not specified, it will generate a random seed.
  *
  *  {s}`maxGenerations: Int = 100`
  *    : Maximum number of spreading generations, where seeds are at generation 0.
  *
  * ## States
  *
  *  {s}`numInfectedNeighbours: Int`
  *    : Number of infected neighbours
  *
  *  {s}`threshold: Double`
  *    : infection threshold for vertex
  *
  *  {s}`infected: Boolean`
  *    : true if vertex is infected, else false
  *
  * ## Returns
  *
  *  | vertex name       | infection status       |
  *  | ----------------- | ---------------------- |
  *  | {s}`name: String` | {s}`infected: Boolean` |
  */
class WattsCascade[T: Threshold](
    infectedSeed: Set[String],
    threshold: T = Threshold.UNIFORM_RANDOM,
    seed: Long = -1,
    maxGenerations: Int = 100
) extends NodeList(Seq("infected")) {

  private val randomiser = if (seed != -1) new Random(seed) else new Random()

  private val get_threshold: () => Double = {
    threshold match {
      case v: Double                         => () => v
      case s: Threshold.RANDOM_SAME_VAL.type =>
        val t = randomiser.nextFloat()
        () => t
      case u: Threshold.UNIFORM_RANDOM.type  => randomiser.nextFloat
    }
  }

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .step { vertex =>
        if (infectedSeed.contains(vertex.name())) {
          vertex.setState("infected", true)
          vertex.messageOutNeighbours(true)
        }
        else
          vertex.setState("infected", false)
        vertex.setState("threshold", get_threshold())
        vertex.setState("numInfectedNeighbours", 0)
      }
      .iterate(
              { vertex =>
                val numInfectedNeighbours =
                  vertex.getState[Int]("numInfectedNeighbours") + vertex.messageQueue[Boolean].size
                vertex.setState("numInfectedNeighbours", numInfectedNeighbours)
                if (vertex.getState[Boolean]("infected"))
                  vertex.voteToHalt()
                else {
                  val degree = vertex.inDegree
                  if (
                          numInfectedNeighbours.toDouble / degree >= vertex
                            .getState[Double]("threshold")
                  ) {
                    vertex.setState("infected", true)
                    vertex.messageOutNeighbours(true)
                  }
                }
              },
              maxGenerations,
              true
      )
}

object WattsCascade {

  def apply[T: Threshold](
      infectedSeed: Iterable[String],
      threshold: T = Threshold.UNIFORM_RANDOM,
      seed: Long = -1,
      maxGenerations: Int = 100
  ) =
    new WattsCascade(infectedSeed.toSet, threshold, seed, maxGenerations)

  sealed trait Threshold[T]

  object Threshold {
    object UNIFORM_RANDOM
    object RANDOM_SAME_VAL
    implicit val threshold: Threshold[Double]                = new Threshold[Double] {}
    implicit val uniform_rnd: Threshold[UNIFORM_RANDOM.type] = new Threshold[UNIFORM_RANDOM.type] {}
    implicit val same_rnd: Threshold[RANDOM_SAME_VAL.type]   = new Threshold[RANDOM_SAME_VAL.type] {}
  }
}
