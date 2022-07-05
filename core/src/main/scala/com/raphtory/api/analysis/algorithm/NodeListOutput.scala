package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class NodeListOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph.select { vertex =>
      val row = vertex.name() +: properties.map(name =>
        vertex.getStateOrElse(name, defaults.getOrElse(name, None), includeProperties = true)
      )
      Row(row: _*)
    }

}
