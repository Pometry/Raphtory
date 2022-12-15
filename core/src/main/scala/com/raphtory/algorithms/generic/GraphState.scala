package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.{Generic, GraphStateOutput}

class GraphState (properties: Seq[String] = Seq.empty[String],
                   defaults: Map[String, Any] = Map.empty[String, Any])
  extends GraphStateOutput(properties,defaults) with Generic {}

object GraphState {
  def apply(properties: Iterable[String] = Seq.empty[String],
            defaults: Map[String, Any] = Map.empty[String, Any]) = new GraphState(properties.toSeq, defaults)

  def apply(properties: String*) = new GraphState(properties)

  def apply(defaults: Map[String, Any], properties: String*) = new GraphState(properties, defaults)
  }