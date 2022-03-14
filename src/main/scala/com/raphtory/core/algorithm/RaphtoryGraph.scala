package com.raphtory.core.algorithm

trait RaphtoryGraph extends GraphOperations[RaphtoryGraph] {
  def transform(f: RaphtoryGraph => RaphtoryGraph): RaphtoryGraph = f(this)
  def transform(algorithm: GraphAlgorithm): RaphtoryGraph
  def execute(f: RaphtoryGraph => Table): Table                   = f(this)
  def execute(algorithm: GraphAlgorithm): Table
}
