package com.raphtory.algorithms

import com.raphtory.algorithms.LPA.lpa
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}
import com.raphtory.core.model.graph.visitor.Vertex

import scala.util.Random

/**
  * Description
  * LPA returns the communities of the constructed graph as detected by synchronous label propagation.
  *Every vertex is assigned an initial label at random. Looking at the labels of its neighbours, a probability is assigned
  *to observed labels following an increasing function then the vertex’s label is updated with the label with the highest
  *probability. If the new label is the same as the current label, the vertex votes to halt. This process iterates until
  *all vertex labels have converged. The algorithm is synchronous since every vertex updates its label at the same time.
  *
  *Parameters
  *top (Int) : The number of top largest communities to return. (Default: 0) If not specified, Raphtory will return all detected communities.
  *weight (String) : Edge property (Default: ""). To be specified in case of weighted graph.
  *maxIter (Int) : Maximum iterations for algorithm to run. (Default: 500)
  *seed (Long) : Value used for the random selection, can be set to ensure same result is returned per run. If not specified, it will generate a random seed.
  *output (String) : The path where the output will be saved. If not specified, defaults to /tmp/LPA
  *
  *Returns
  *ID (Long) : Vertex ID
  *Label (Long) : The ID of the community this belongs to
  *
  *Notes
  *This implementation of LPA incorporated probabilistic elements which makes it
  *non-deterministic; The returned communities may differ on multiple executions.
  *Which is why you may want to set the seed if testing.
  **/
class LPA(top: Int= 0, weight: String= "", maxIter: Int = 500, seed:Long= -1, output:String= "/tmp/LPA") extends GraphAlgorithm {

  val rnd: Random = if (seed == -1) new scala.util.Random else new scala.util.Random(seed)
  val SP = 0.2F // Stickiness probability

  override def apply(graph: GraphPerspective): GraphPerspective = {
    graph.step({
      vertex =>
        val lab = rnd.nextLong()
        vertex.setState("lpalabel", lab)
        vertex.messageAllNeighbours((vertex.ID(), lab))
    }).iterate({
      vertex =>
        lpa(vertex, weight, SP, rnd)
    }, maxIter, false)
  }

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({

      vertex =>
        Row(
          vertex.ID(),
          vertex.getState("lpalabel"),
        )
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }
  // TODO AGGREGATION STATS - See old code in old dir
}

object LPA{
  def apply(top:Int = 0, weight:String = "", maxIter:Int = 500, seed:Long = -1, output:String = "/tmp/LPA") = new LPA(top, weight, maxIter, seed, output)
  def lpa(vertex: Vertex, weight: String, SP: Double, rnd:Random): Unit = {
    val vlabel = vertex.getState[Long]("lpalabel")
    val vneigh = vertex.getEdges()
    val neigh_freq = vneigh.map { e => (e.ID(), e.getProperty(weight).getOrElse(1.0F)) }
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum)
    // Process neighbour labels into (label, frequency)
    val gp = vertex.messageQueue[(Long, Long)].map { v => (v._2, neigh_freq.getOrElse(v._1, 1.0F))}
    // Get label most prominent in neighborhood of vertex
    val maxlab = gp.groupBy(_._1).mapValues(_.map(_._2).sum)
    var newLabel =  maxlab.filter(_._2 == maxlab.values.max).keySet.max
    // Update node label and broadcast
    if (newLabel == vlabel)
      vertex.voteToHalt()
    newLabel =  if (rnd.nextFloat() < SP) vlabel else newLabel
    vertex.setState("lpalabel", newLabel)
    vertex.messageAllNeighbours((vertex.ID(), newLabel))
  }
}

