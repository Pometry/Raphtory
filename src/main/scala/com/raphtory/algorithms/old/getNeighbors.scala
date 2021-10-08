package com.raphtory.algorithms.old

import com.raphtory.core.model.algorithm.Analyser

/**
  * Gets neighbors of node n.
**/
object getNeighbors {
  def apply(node: String, name: String): getNeighbors = new getNeighbors(Array(node,name))
}

class getNeighbors(args: Array[String]) extends Analyser[List[String]](args) {
  val node: String = args.head
  val name: String = if (args.length < 2) "Word" else args(1)

  override def setup(): Unit =
    view.getVertices().filter(v => v.getPropertyValue(name).getOrElse(v.ID()).toString == node).foreach { vertex =>
      vertex.messageAllNeighbours(true)
    }

  override def analyse(): Unit =
    view.getMessagedVertices().foreach { vertex =>
      val msgQ = vertex.messageQueue[Boolean].foldRight(false)((a, b) => b | a)
      if (msgQ)
        vertex.setState("neighbor", true)
    }

  override def returnResults(): List[String] = {
    val nodes = view.getVertices().filter(v => v.getOrSetState[Boolean]("neighbor", false)).toList
    if (nodes.isEmpty) List()
    else
      nodes.map(v => v.getPropertyValue(name).getOrElse(v.ID()).toString)
  }
  override def defineMaxSteps(): Int = 10

  override def extractResults(results: List[List[String]]): Map[String, Any] = {
    val endResults = results.flatten
    Map("node"-> node, "neighbors"-> endResults)
  }
}
