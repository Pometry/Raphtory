package com.raphtory.core.algorithm

trait TemporalGraph extends GraphPerspectiveSet {
  def from(startTime: Long): TemporalGraph
  def from(startTime: String): TemporalGraph
  def until(endTime: Long): TemporalGraph
  def until(endTime: String): TemporalGraph
  def slice(startTime: Long, endTime: Long): TemporalGraph
  def slice(startTime: String, endTime: String): TemporalGraph
  def raphtorize(increment: Long): GraphPerspectiveSet
  def raphtorize(increment: String): GraphPerspectiveSet
  def raphtorize(increment: Long, window: Long): GraphPerspectiveSet
  def raphtorize(increment: String, window: String): GraphPerspectiveSet
  def raphtorize(increment: Long, windows: List[Long]): GraphPerspectiveSet
  def raphtorize(increment: String, windows: List[String]): GraphPerspectiveSet
}
