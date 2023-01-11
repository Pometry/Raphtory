package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

abstract class NodeListOutput(
    properties: Seq[String] = Seq.empty[String],
    defaults: Map[String, Any] = Map.empty[String, Any]
) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table =
    graph
      .select { vertex =>
        val propertiesAndStates =
          if (properties.isEmpty) {
            val x = vertex.getPropertySet() ++ vertex.getStateSet()
            KeyPair("name", vertex.name()) +: x.map { key =>
              KeyPair(key, vertex.getStateOrElse(key, "", includeProperties = true))
            }
          }
          else
            KeyPair("name", vertex.name()) +: properties.collect {
              case key: String if !key.equals("name") =>
                KeyPair(key, vertex.getStateOrElse(key, defaults.getOrElse(key, None), includeProperties = true))
            }
        Row(propertiesAndStates: _*)
      }
}
