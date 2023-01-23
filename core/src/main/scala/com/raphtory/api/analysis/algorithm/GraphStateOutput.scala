package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class GraphStateOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph
      .setGlobalState { state =>
        properties.map { key =>
          state.newConstant[Any](
                  key,
                  state.get(key) match {
                    case Some(acc) => acc.value
                    case None      => defaults.getOrElse(key, None)
                  }
          )
        }
      }
      .globalSelect(properties: _*)
}
