package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.internals.communication.SchemaProviderInstances.genericSchemaProvider

/**
  * {s}`HITS(iterateSteps: Int = 100, truncate: Boolean = false, tol: Double = 0.00001)`
  * : Calculate the hub and authority scores of each vertex in the graph.
  *
  * The HITS algorithm is similar to pagerank but imagines 2 types of nodes: good hubs which have outgoing edges linking to lots of good authorities, and good authorities which have incoming edges from lots of good hubs.
  * https://en.wikipedia.org/wiki/HITS_algorithm
  *
  * * ## Parameters
  *
  *  {s}`iterateSteps: Int = 100`
  *    : Maximum number of iterations for the algorithm to run.
  *  {s}`truncate: Boolean = false`
  *    : If set to true, the scores at the end are truncated to 8 dp. s
  *  {s}`tol: Double = 0.00001`
  *    : Each vertex will vote to halt when it changes by less than tol in that iteration.
  *
  * ## States
  *
  * {s}`hitshub: Long`
  * : The current hub score of a vertex.
  * {s}`hitsauth: Long`
  * : The current authority score of a vertex
  *
  * ## Returns
  *
  * | vertex name       | hubs score          | authorities score   |
  * | ----------------- | ------------------- | ------------------- |
  * | {s}`name: String` | {s}`husbauth: Long` | {s}`hitsauth: Long` |
  *
  * ## Implementation
  *
  * The algorithm works by each vertex calculating its hub score by the sum of the authorities scores from its out neighbours, and its authorities score by the sum of the hubs score from its in neighbours, sending out its new scores, and iterating.
  *
  * 1. Initially we create accumulators, set the hub scores to an initial value (1.0), and message each vertex's hub score to its out neighbours.
  *
  * 2. Each vertex calculates it's initial authorities score from the sum of the hub scores it receives, and messages its hub score to its out neighbours, and its authorities score to its in neighbours.
  *
  * 3. On each iteration each vertex calculates its hub score from the sum of the authorities scores it receives, its authorities score from the sum of the hub values it receives, and normalises these values: dividing by the maximum of all the hubs and authorities scores from the last iteration.
  *
  * 4. It then messages its new hub score to its out neighbours, and its authorities score to its neighbours.
  *
  * 5. This repeats until the scores stay approximately constant, in which case the scores are normalised one more time.
  */
class HITS(iterateSteps: Int = 100, tol: Double = 1e-4) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = {
    val initHub = 1.0

    graph
      .setGlobalState { state =>
        state.newMax[Double]("hubAuthMax", retainState = false)
        state.newMax[Double]("hubMax", retainState = false)
        state.newMax[Double]("authMax", retainState = false)
      }
      .step { vertex =>
        // Initialise the hub values and propagate
        vertex.setState("hitshub", initHub)

        val outDegree = vertex.outDegree
        if (outDegree > 0.0)
          vertex.messageOutNeighbours(HubMsg(initHub))
      }
      .step { (vertex, state) =>
        // Initialise the auth values based on recieved hub values
        val firstHub = vertex.getState[Double]("hitshub") // fix so not double, msg types

        val queue     = vertex.messageQueue[HubMsg]
        val firstAuth = queue.map(_.value).sum / initHub // normalise
        vertex.setState("hitsauth", firstAuth)

        state("hubAuthMax") += initHub
        state("hubAuthMax") += firstAuth

        val outDegree = vertex.outDegree
        val inDegree  = vertex.inDegree

        if (outDegree > 0.0)
          vertex.messageOutNeighbours(HubMsg(firstHub))
        if (inDegree > 0.0)
          vertex.messageInNeighbours(AuthMsg(firstAuth))
      }
      .iterate(
              // set the new hub values to the sum of the received auth values and vice versa
              // store the maximum of both in an accumulator, so the values can be normalised by dividing by the max
              { (vertex, state) =>
                val hubAuthMax: Double = state("hubAuthMax").value

                val currHub  = vertex.getState[Double]("hitshub")
                val currAuth = vertex.getState[Double]("hitsauth")
                val queue    = vertex.messageQueue[Message]

                var newAuth: Double = 0.0
                var newHub: Double  = 0.0

                queue.foreach {
                  case AuthMsg(value) =>
                    newHub += (value / hubAuthMax) // normalise

                  case HubMsg(value)  =>
                    newAuth += (value / hubAuthMax)
                }

                state("hubAuthMax") += newHub
                state("hubAuthMax") += newAuth

                vertex.setState("hitshub", newHub)
                vertex.setState("hitsauth", newAuth)

                val outDegree = vertex.outDegree
                val inDegree  = vertex.inDegree

                if (outDegree > 0.0)
                  vertex.messageOutNeighbours(HubMsg(newHub))
                if (inDegree > 0.0)
                  vertex.messageInNeighbours(AuthMsg(newAuth))

                if (Math.abs(currHub - newHub) < tol & Math.abs(currAuth - newAuth) < tol)
                  vertex.voteToHalt()
              },
              iterations = iterateSteps,
              executeMessagedOnly = false
      )
      .step { (vertex, state) =>
        // store the hubMax, authMax in an accumulator to normalise the values separately at the end (rather than normalising by the max of these 2 vals)
        // we only need this value at the end so don't need these calculations earlier
        val currHub  = vertex.getState[Double]("hitshub")
        val currAuth = vertex.getState[Double]("hitsauth")

        state("hubMax") += currHub
        state("authMax") += currAuth
      }
      .step { (vertex, state) =>
        // normalise the final values
        val hubMax: Double  = state("hubMax").value
        val authMax: Double = state("authMax").value

        val currHub  = vertex.getState[Double]("hitshub")
        val currAuth = vertex.getState[Double]("hitsauth")

        var newAuth = currAuth / authMax
        var newHub  = currHub / hubMax

        vertex.setState("hitshub", newHub)
        vertex.setState("hitsauth", newAuth)
      }

  }

  override def tabularise(graph: GraphPerspective): Table =
    graph.select("name", "hitshub", "hitsauth")

  sealed trait Message {}
  case class HubMsg(value: Double)  extends Message
  case class AuthMsg(value: Double) extends Message
}

object HITS {

  def apply(iterateSteps: Int = 100, tol: Double = 1e-4) =
    new HITS(iterateSteps, tol)
}
