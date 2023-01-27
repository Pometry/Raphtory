package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class NodeListOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph
      .step { vertex =>
        vertex.getOrSetState("name", vertex.name())
        defaults.foreach {
          case (key, value) =>
            vertex.getOrSetState(key, value)
        }
      }
      .select("name" +: properties: _*)
}
