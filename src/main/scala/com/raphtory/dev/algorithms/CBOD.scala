package com.raphtory.dev.algorithms

import com.raphtory.algorithms.old.Analyser
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.immutable

object CBOD {
  def apply(args: Array[String]): CBOD = new CBOD(args)
}

class CBOD(args: Array[String]) extends Analyser[Any](args) {
  //args = [top , edge property, maxIter, cutoff]

  val arg: Array[String] = args.map(_.trim)

  val top: Int         = if (arg.length == 0) 0 else arg.head.toInt
  val weight: String       = if (arg.length < 2) "" else arg(1)
  val maxIter: Int       = if (arg.length < 3) 500 else arg(2).toInt
  private val debug             = false //for printing debug messages

  val cutoff: Double = if (args.length < 4) 0.0 else args(3).toDouble
  val filter: Array[Long] = if (args.length < 5) Array() else args(4).split("-").map(_.toLong)
  val output_file: String = ""
  val nodeType: String    = ""
  var b0 = System.currentTimeMillis()
  override def setup(): Unit =
    view.getVertices().foreach { vertex =>
      val lab = (scala.util.Random.nextLong(), scala.util.Random.nextLong())
      vertex.setState("lpalabel", lab)
      vertex.messageAllNeighbours((vertex.ID(), lab._2))
    }

  override def analyse(): Unit = {
    val t0 = System.currentTimeMillis()
    if (debug)
      println(
        s"{BtwSuperstep: ${view.superStep}, ExecTime: ${t0 - b0}}"
      )
    view.getMessagedVertices().foreach { vertex =>
      val t1 = System.currentTimeMillis()
      try {
        val Vlabel = vertex.getState[(Long, Long)]("lpalabel")._2
        val Oldlabel = vertex.getState[(Long, Long)]("lpalabel")._1

        // Get neighbourhood Frequencies -- relevant to weighted LPA
        val vneigh = vertex.getOutEdges() ++ vertex.getInEdges()
        val neigh_freq = if (weight.isEmpty)
          (vneigh.map(_.ID()).toSet zip Array.fill(vneigh.toSet.size)(1L)).toMap
        else vneigh
          .map(e => (e.ID(), e.getProperty(weight).getOrElse(1L).asInstanceOf[Long]))
          .groupBy(_._1)
          .mapValues(x => x.map(_._2).sum / x.size)

        // Process neighbour labels into (label, frequency)
        val gp = vertex.messageQueue[(Long, Long)].map(v => (v._2, neigh_freq(v._1)))

        // Get label most prominent in neighborhood of vertex
        val maxlab = gp.groupBy(_._1).mapValues(_.map(_._2).sum)

        // Update node label and broadcast
        val newLabel = maxlab.filter(_._2 == maxlab.values.max).keySet.max
        val nlab = newLabel match {
          case Vlabel | Oldlabel =>
            vertex.voteToHalt()
            (List(Vlabel, Oldlabel).min, List(Vlabel, Oldlabel).max)
          case _ => (Vlabel, newLabel)
        }
        vertex.setState("lpalabel", nlab)
        vertex.messageAllNeighbours((vertex.ID(), nlab._2))
        doSomething(vertex, gp.map(_._1).toArray)
      } catch {
        case e: Exception => println(e, vertex.ID())
      }
      if (filter.contains(vertex.ID()) & debug)
        println(
          s"{Superstep: ${view.superStep}, vID: ${vertex.ID()},ExecTime: ${System.currentTimeMillis() - t1}}"
        )
    }
    if (debug)
    println(
      s"{Superstep: ${view.superStep},  ExecTime: ${System.currentTimeMillis() - t0}}"
    )
    b0 = System.currentTimeMillis()
  }
//  def doSomething(v: VertexVisitor, gp: Array[Long]): Unit = {}

  def doSomething(v: Vertex, neighborLabels: Array[Long]): Unit = {
    val vlabel       = v.getState[(Long, Long)]("lpalabel")._2
    val outlierScore = 1 - (neighborLabels.count(_ == vlabel) / neighborLabels.length.toDouble)
    v.setState("outlierscore", outlierScore)
  }

  override def returnResults(): Any =
    view
      .getVertices()
      .filter(v => v.Type() == nodeType)
      .map(vertex => (vertex.getProperty("ID").getOrElse("Unknown"), vertex.getOrSetState[Double]("outlierscore", -1.0)))

  override def extractResults(results: List[Any]): Map[String,Any] = {
    val endResults = results.asInstanceOf[ArrayBuffer[immutable.ParHashMap[String, Double]]].flatten
//
//    val sorted  = endResults.filter(_._2 > cutoff)
////    val sorted    = outliers.sortBy(-_._2)
//    val sortedstr = sorted.map(x => s""""${x._1}":${x._2}""")
////    val top5       = sorted.map(_._1).take(5)
//    val total     = sorted.length
//    val out       = if (top == 0) sortedstr else sortedstr.take(top)
//    val text = s"""{"time":$timestamp,"total":$total,"outliers":{${out
//      .mkString(",")}},"viewTime":$viewCompleteTime}"""
    Map[String,Any]()
  }


  override def defineMaxSteps(): Int = maxIter
}
