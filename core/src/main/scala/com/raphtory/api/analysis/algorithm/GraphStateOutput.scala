package com.raphtory.api.analysis.algorithm
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.table.Row

abstract class GraphStateOutput(
  properties: Seq[String] = Seq.empty[String],
  defaults: Map[String, Any] = Map.empty[String, Any]
  ) extends BaseAlgorithm {

  override def tabularise(graph: Out): Table = {
    graph.globalSelect{
      state =>
        val row = properties.map{name =>
        state.get(name) match {
          case Some(acc) => acc.value
          case None => defaults.getOrElse(name,None)
        }

        }
        Row(row:_*)
    }
  }
  }