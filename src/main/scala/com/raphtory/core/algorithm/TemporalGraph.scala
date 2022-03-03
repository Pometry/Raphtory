package com.raphtory.core.algorithm

trait TemporalGraph extends GraphPerspective {
  def from(startTime: Long): TemporalGraph
  def until(endTime: Long): TemporalGraph
  def slice(startTime: Long, endTime: Long): TemporalGraph
  def raphtorize(increment: Long): GraphPerspective
  def raphtorize(increment: Long, window: Long): GraphPerspective
  def raphtorize(increment: Long, windows: List[Long]): GraphPerspective
  def execute(f: TemporalGraph => TemporalGraph): TemporalGraph       = f(this)
  def execute(f: TemporalGraph => GraphPerspective): GraphPerspective = f(this)
  def execute(f: TemporalGraph => Table): Table                       = f(this)
}
