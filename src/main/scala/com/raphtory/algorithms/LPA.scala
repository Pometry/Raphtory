package com.raphtory.algorithms

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.analysis.entity.Vertex

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.ParArray
import scala.tools.nsc.io.Path

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

Returns
  total (Int)     – Number of detected communities.
  communities (List(List(Long))) – Communities sorted by their sizes. Returns largest top communities if specified.

Notes
  This implementation of LPA incorporated probabilistic elements which makes it non-deterministic;
  The returned communities may differ on multiple executions.
  **/
object LPA {
  def apply(args: Array[String]): LPA = new LPA(args)
}

class LPA(args: Array[String]) extends Analyser[Any](args) { //im: change type
  //args = [top output, edge property, max iterations]

  val arg: Array[String] = args.map(_.trim)

  val top: Int         = if (arg.length == 0) 0 else arg.head.toInt
  val weight: String       = if (arg.length < 2) "" else arg(1)
  val maxIter: Int       = if (arg.length < 3) 500 else arg(2).toInt
  val rnd    = new scala.util.Random

  val output_file: String = System.getenv().getOrDefault("LPA_OUTPUT_PATH", "").trim
  val SP = 0.2F // Stickiness probability

  override def setup(): Unit = {
    view.getVertices().foreach { vertex =>
      val lab = rnd.nextLong()
      vertex.setState("lpalabel", lab)
      vertex.messageAllNeighbours((vertex.ID(),lab))
    }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      try {
        val vlabel = vertex.getState[Long]("lpalabel")

        // Get neighbourhood Frequencies -- relevant to weighted LPA
        val vneigh = vertex.getOutEdges ++ vertex.getIncEdges
        val neigh_freq = vneigh.map { e => (e.ID(), e.getPropertyValue(weight).getOrElse(1.0F).asInstanceOf[Float]) }
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
        doSomething(vertex, gp.map(_._1).toArray)
      } catch {
        case e: Exception => println(e, vertex.ID())
      }
    }
  }


  def doSomething(v: Vertex, gp: Array[Long]): Unit = {}

  override def returnResults(): Any =
    view.getVertices()
      //.filter(v => v.Type() == nodeType)
      .map(vertex => (vertex.getState[Long]("lpalabel"), vertex.ID()))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2).toList))

  override def extractResults(results: List[Any]): Map[String, Any] = {
    val er      = extractData(results)
    val commtxt = er.communities.map(x => s"""["${x.mkString("\",\"")}"]""")

    Map[String,Any]("top5"->er.top5,"total"->er.total,"totalIslands"->er.totalIslands,
       "communities"->commtxt)
  }

  def extractData(results: List[Any]): fd = {
    val endResults = results.asInstanceOf[List[immutable.ParHashMap[Long, List[String]]]]
    try {
      val grouped             = endResults.flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2))
      val groupedNonIslands   = grouped.filter(x => x._2.size > 1)
      val sorted              = grouped.toArray.sortBy(_._2.size)(sortOrdering)
      val top5                = sorted.map(_._2.size).take(5)
      val total               = grouped.size
      val totalWithoutIslands = groupedNonIslands.size
      val totalIslands        = total - totalWithoutIslands
      val communities         = if (top == 0) sorted.map(_._2) else sorted.map(_._2).take(top)
      fd(top5, total, totalIslands, communities)
    } catch {
      case _: UnsupportedOperationException => fd(Array(0), 0, 0, Array(List("0")))
    }
  }

  override def defineMaxSteps(): Int = maxIter

}

case class fd(top5: Array[Int], total: Int, totalIslands: Int, communities: Array[List[String]])