package com.raphtory.core.model.graph

import com.raphtory.core.model.graph.visitor.Vertex

abstract class GraphLens(jobId: String,
                         timestamp: Long,
                         window: Option[Long]) {
//TODO REMOVE THESE STUBS ONCE ANALYSERS CLEANED
  def getVertices():List[Vertex] = {List()}
  def getMessagedVertices():List[Vertex] = {List()}

}
