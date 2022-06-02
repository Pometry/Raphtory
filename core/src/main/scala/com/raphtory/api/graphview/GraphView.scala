package com.raphtory.api.graphview

import com.raphtory.api.algorithm.Generic
import com.raphtory.api.algorithm.GenericReduction
import com.raphtory.api.algorithm.GenericallyApplicable
import com.raphtory.api.algorithm.Multilayer
import com.raphtory.api.algorithm.MultilayerProjection
import com.raphtory.api.algorithm.MultilayerReduction
import com.raphtory.api.table.Table

trait GraphView extends GraphPerspective {
  def transform(algorithm: Generic): Graph

  def transform(algorithm: MultilayerProjection): MultilayerGraph

  def transform(algorithm: GenericReduction): ReducedGraph

  def execute(algorithm: GenericallyApplicable): Table
}

trait MultilayerGraphView extends MultilayerGraphPerspective with GraphView {
  def transform(algorithm: Multilayer): Graph

  def transform(algorithm: MultilayerReduction): ReducedGraph
  def execute(algorithm: Multilayer): Table

  def execute(algorithm: MultilayerReduction): Table
}
