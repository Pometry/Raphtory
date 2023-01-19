package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class GraphStateOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph.globalSelect { state =>
      val row = properties.map { key =>
        val getProps = state.get(key) match {
          case Some(acc) => acc.value
          case None      => defaults.getOrElse(key, None)
        }
        key -> getProps
      }

      Row(row: _*)
    }
}
