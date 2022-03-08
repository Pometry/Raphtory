package com.raphtory.core.algorithm

trait TemporalGraph extends GraphPerspectiveSet {
  def from(startTime: Long): TemporalGraph
  def until(endTime: Long): TemporalGraph
  def slice(startTime: Long, endTime: Long): TemporalGraph
  def raphtorize(increment: Long): GraphPerspectiveSet
  def raphtorize(increment: Long, window: Long): GraphPerspectiveSet
  def raphtorize(increment: Long, windows: List[Long]): GraphPerspectiveSet
}
