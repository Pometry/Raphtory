package com.raphtory.algorithms.old

import com.raphtory.core.model.algorithm.Analyser
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable

/**
  *Gets the 2-hop network for node n.
**/
object twoHopNeighbors {
  def apply(node: String, name: String,weight: String, neighborSize: Int) :
  twoHopNeighbors = new twoHopNeighbors(Array(node,name, neighborSize.toString, weight))
}

class twoHopNeighbors(args: Array[String]) extends Analyser[List[(String, mutable.Map[Int, List[String]])]](args) {
  var node: String     = args.head
  val name: String     = if (args.length < 2) "" else args(1)
  val neiSize: Int     = if (args.length < 3) 0 else args(2).toInt
  val property: String = if (args.length < 4) "weight" else args(3)

  override def setup(): Unit =
    view.getVertices()
      .filter(v => v.getPropertyValue(name).getOrElse(v.ID()).toString == node)
      .foreach { vertex =>
        vertex.setState("state", mutable.Map((2, List())))
        messageSelect(vertex, 2)
      }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val msgQ   = vertex.messageQueue[(String, Int)]
      val msq    = msgQ.groupBy(_._2).map(f => (f._1 - 1, f._2.map(_._1)))
      val st     = mutable.Map(msq.toSeq: _*)
      val prevst = vertex.getOrSetState[mutable.Map[Int, List[String]]]("state", mutable.Map[Int, List[String]]())
      prevst.foreach(x => st(x._1) = x._2 ++ st.getOrElse(x._1, List[String]()))
      vertex.setState("state", st)
      val pos = st.keys.min
      if (pos > 0)        messageSelect(vertex, pos)
    }

  override def returnResults(): List[(String, mutable.Map[Int, List[String]])] =
    view
      .getVertices()
      .map(vertex =>
        (
                vertex.getPropertyValue(name).getOrElse(vertex.ID()).toString,
                vertex.getOrSetState[mutable.Map[Int, List[String]]]("state", mutable.Map[Int, List[String]]())
        )
      )
      .filterNot(f => f._2.isEmpty)
      .toList

  override def defineMaxSteps(): Int = 10

  override def extractResults(results: List[List[(String, mutable.Map[Int, List[String]])]]): Map[String, Any] = {
    val endResults = results.flatten.groupBy(f => f._1).mapValues(x => x.flatMap(_._2).toMap)
    val root       = (node, getHop(endResults,1).filter(ch => ch._2(1).contains(node)).keySet.toList)
    val leaf       = root._2.filterNot(_ == node).map(wd => (wd, getHop(endResults,0)
      .filter(ch => ch._2(0).contains(wd)).keySet.toList))
    (root +: leaf).toMap
  }
  def getHop(A:Map[String,Map[Int, List[String]]], h: Int): Map[String,Map[Int, List[String]]] =
    A.filter(ch => ch._2.keys.toArray.contains(h))
  def messageSelect(vertex: Vertex, pos: Int): Unit = {
    val wd = vertex.getPropertyValue(name).getOrElse(vertex.ID()).toString
    var nei = vertex.getEdges()
      .map(e => e.ID() -> e.getPropertyValue(property).getOrElse(1.0f).asInstanceOf[Float])
      .groupBy(_._1)
      .mapValues(_.map(_._2).max)
      .toArray
    nei = if (neiSize == 0) nei else nei.sortBy(-_._2).take(neiSize)
    nei.map(_._1).foreach(x => vertex.messageNeighbour(x, (wd, pos)))
  }
}
