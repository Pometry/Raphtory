package com.raphtory.algorithms

import com.raphtory.api.Analyser
import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable
import scala.collection.parallel.mutable.ParArray
import scala.reflect.io.Path
import scala.util.Random

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

class LPA(args: Array[String]) extends Analyser(args) {
  //args = [top output, edge property, max iterations]

  val arg: Array[String] = args.map(_.trim)

  val top: Int         = if (arg.length == 0) 0 else arg.head.toInt
  val weight: String       = if (arg.length < 2) "" else arg(1)
  val maxIter: Int       = if (arg.length < 3) 500 else arg(2).toInt


  val output_file: String = System.getenv().getOrDefault("LPA_OUTPUT_PATH", "").trim
  val nodeType: String    = System.getenv().getOrDefault("NODE_TYPE", "").trim

  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val lab = (scala.util.Random.nextLong(), scala.util.Random.nextLong())
      vertex.setState("lpalabel", lab)
      vertex.messageAllNeighbours((vertex.ID(), lab._2))
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      try {
        val Vlabel   = vertex.getState[(Long, Long)]("lpalabel")._2
        val Oldlabel = vertex.getState[(Long, Long)]("lpalabel")._1

        // Get neighbourhood Frequencies -- relevant to weighted LPA
        val vneigh = vertex.getOutEdges ++ vertex.getIncEdges
        val neigh_freq = vneigh
          .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1L).asInstanceOf[Long]))
          .groupBy(_._1)
          .mapValues(x => x.map(_._2).sum / x.size)

        // Process neighbour labels into (label, frequency)
        val mq = vertex.messageQueue[(Long, Long)]
        val gp = mq.map(v => (v._2, neigh_freq(v._1)))
        // Get label most prominent in neighborhood of vertex
        val maxlab = gp.groupBy(_._1).mapValues(_.map(_._2).sum)

        // Update node label and broadcast
        val newLabel = maxlab.filter(_._2 == maxlab.values.max).keySet.max
        val nlab = newLabel match {
          case Vlabel =>
            vertex.voteToHalt()
            (Vlabel, Vlabel)
          case Oldlabel =>
            if (Random.nextFloat() < 0.1) (Vlabel, Vlabel) else (List(Vlabel, Oldlabel).min, List(Vlabel, Oldlabel).max)
          case _ => (Vlabel, newLabel)
        }
        vertex.setState("lpalabel", nlab)
        vertex.messageAllNeighbours((vertex.ID(), nlab._2))
        doSomething(vertex, gp.map(_._1).toArray)
      } catch {
        case e: Exception => println(e, vertex.ID())
      }
    }

  def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {}

  override def returnResults(): Any =
    view
      .getVertices()
      .filter(v => v.Type() == nodeType)
      .map(vertex => (vertex.getState[(Long, Long)]("lpalabel")._2, vertex.ID()))
      .groupBy(f => f._1)
      .map(f => (f._1, f._2.map(_._2)))

  override def processResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {
    val er      = extractData(results)
    val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
    val text = s"""{"time":$timestamp,"top5":[${er.top5
      .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"""+
       s""""communities": [${commtxt.mkString(",")}],"""+
      s""""viewTime":$viewCompleteTime}"""
    output_file match {
      case "" => println(text)
      case "mongo" => publishData(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
  }

  override def processWindowResults(
      results: ArrayBuffer[Any],
      timestamp: Long,
      windowSize: Long,
      viewCompleteTime: Long
  ): Unit = {
    val er      = extractData(results)
    val commtxt = er.communities.map(x => s"""[${x.mkString(",")}]""")
    val text = s"""{"time":$timestamp,"windowsize":$windowSize,"top5":[${er.top5
      .mkString(",")}],"total":${er.total},"totalIslands":${er.totalIslands},"""+
      s""""communities": [${commtxt.mkString(",")}],"""+
      s""""viewTime":$viewCompleteTime}"""
    output_file match {
      case "" => println(text)
      case "mongo" => publishData(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
    publishData(text)
  }

  def extractData(results: ArrayBuffer[Any]): fd = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[Long, ParArray[String]]]]
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
      case _: UnsupportedOperationException => fd(Array(0), 0, 0, Array(ArrayBuffer("0")))
    }
  }

  override def defineMaxSteps(): Int = maxIter

}

case class fd(top5: Array[Int], total: Int, totalIslands: Int, communities: Array[ArrayBuffer[String]])
