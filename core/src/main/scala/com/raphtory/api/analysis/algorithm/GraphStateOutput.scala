package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class GraphStateOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table = {
    graph.globalSelect{
      state =>
        val getProps =
          properties.map { name =>
            state.get(name) match {
              case Some(acc) => acc.value
              case None => defaults.getOrElse(name, None)
            }
          }
        val row = Map("properties" -> getProps)

        Row(row)
    }
}
