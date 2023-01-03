package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class NodeListOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph.select { vertex =>
      val row = if (properties.isEmpty) {
        val propertySet: List[String] = vertex.getPropertySet()
        val stateSet: List[String] = vertex.getStateSet()
        val properties = propertySet.map(key => vertex.getStateOrElse(key, defaults.getOrElse(key, None), includeProperties = true))
        val states = stateSet.map(key => vertex.getStateOrElse(key, defaults.getOrElse(key, None), includeProperties = true))
        vertex.name() +: properties +: states
      } else {
        vertex.name() +: properties.map(name =>
          vertex.getStateOrElse(name, defaults.getOrElse(name, None), includeProperties = true)
        )
      }
      Row(row: _*)
    }

}
