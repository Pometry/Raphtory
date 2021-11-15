package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row}

/**
Description
  LPA returns the communities of the constructed graph as detected by synchronous label propagation.
  Every vertex is assigned an initial label at random. Looking at the labels of its neighbours, a probability is assigned
  to observed labels following an increasing function then the vertex’s label is updated with the label with the highest
  probability. If the new label is the same as the current label, the vertex votes to halt. This process iterates until
  all vertex labels have converged. The algorithm is synchronous since every vertex updates its label at the same time.

Parameters
  top (Int)       – The number of top largest communities to return. (default: 0)
                      If not specified, Raphtory will return all detected communities.
  weight (String) - Edge property (default: ""). To be specified in case of weighted graph.
  maxIter (Int)   - Maximum iterations for LPA to run. (default: 500)
  seed    (Long)  - The seed for randomness, if empty then random seed is selected by system
Returns
  total (Int)     – Number of detected communities.
  communities (List(List(Long))) – Communities sorted by their sizes. Returns largest top communities if specified.

Notes
  This implementation of LPA incorporated probabilistic elements which makes it non-deterministic;
  The returned communities may differ on multiple executions.
  **/
class LPA(top: Int= 0, weight: String= "", maxIter: Int = 500, seed:Long= -1, output:String= "/tmp/LPA") extends GraphAlgorithm {

  val rnd    = if (seed == -1) new scala.util.Random else new scala.util.Random(seed)
  val SP = 0.2F // Stickiness probability

  override def algorithm(graph: GraphPerspective): Unit = {
    graph.step({
      vertex =>
        val lab = rnd.nextLong()
        vertex.setState("lpalabel", lab)
        vertex.messageAllNeighbours((vertex.ID(),lab))
    }).iterate({
      vertex =>
        val vlabel = vertex.getState[Long]("lpalabel")
        val vneigh = vertex.getEdges()
        val neigh_freq = vneigh.map { e => (e.ID(), e.getProperty(weight).getOrElse(1.0F).asInstanceOf[Float]) }
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
    }, maxIter,false).select({
      vertex => Row(
        vertex.ID(),
        vertex.getState("lpalabel"),
      )
    }).writeTo(output)
  }
  // TODO AGGREGATION STATS - See old code in old dir


}


object LPA{
  def apply(top:Int = 0, weight:String = "", maxIter:Int = 500, seed:Long = -1, output:String = "/tmp/LPA") = new LPA(top, weight, maxIter, seed, output)
}
