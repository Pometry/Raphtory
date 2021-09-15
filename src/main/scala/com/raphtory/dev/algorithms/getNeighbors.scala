package com.raphtory.dev.algorithms

import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.graph.visitor.Vertex

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.io.Path

/**
  * Gets neighbors of node n.
**/
class getNeighbors(args: Array[String]) extends Analyser[Any](args) {
  val node: String       = if (args.length < 1) "" else args.head
  val name: String       = if (args.length < 3) "Word" else args(2)
  val output_file: String  = System.getenv().getOrDefault("OUTPUT_PATH", "").trim


  override def setup(): Unit = {
    view.getVertices()
      .filter(v =>v.getPropertyValue(name).getOrElse(v.ID()).toString==node)
    .foreach {
    vertex =>
      vertex.messageAllNeighbours(true)
  }
  }

  override def analyse(): Unit = {
    view.getMessagedVertices().foreach { vertex =>
      val msgQ = vertex.messageQueue[Boolean].foldRight(false)((a, b) => b | a)
      if (msgQ)
        vertex.setState("neighbor", true)
    }
  }

  override def returnResults(): Any = {
    val nodes = view
      .getVertices()
      .filter(v => v.getOrSetState[Boolean]("neighbor", false)).toList
    if (nodes.isEmpty)  List() else
      nodes.map{v=> v.getPropertyValue(name).getOrElse(v.ID()).toString}
  }
  override def defineMaxSteps(): Int = 10

  override def extractResults(results: List[Any]): Map[String, Any] = {
    val endResults = results
      .asInstanceOf[List[List[String]]]
      .flatten
    val text =
      s"""{$node : [${endResults.mkString(",")}]}"""
    output_file match {
      case "" => println(text)
      case _  => Path(output_file).createFile().appendAll(text + "\n")
    }
    Map[String, Any]()
  }
}