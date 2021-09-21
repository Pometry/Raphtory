package com.raphtory.core.model.graph

import com.raphtory.core.model.graph.visitor.Vertex

abstract class GraphPerspective(jobId: String,
                                timestamp: Long,
                                window: Option[Long]) {

  def getVertices():List[Vertex]
  def getMessagedVertices():List[Vertex]

}
