package com.raphtory.dev.stackoverflow

import com.raphtory.core.actors.graphbuilder.GraphBuilder

class SOBuilder extends GraphBuilder[String]{
  override protected def parseTuple(tuple: String): Unit = {
    val line = tuple.split(" ")
    val sourceNode = line(0).toLong
    val destNode = line(1).toLong
    val timestamp = line(2).toLong
    if (sourceNode != destNode) {
      addVertex(timestamp,sourceNode)
      addVertex(timestamp,destNode)
      addEdge(timestamp,sourceNode,destNode)
    }
  }
}
