package com.raphtory.core.algorithm

trait TemporalGraph extends RaphtoryGraph {
  def from(startTime: Long): TemporalGraph
  def from(startTime: String): TemporalGraph
  def until(endTime: Long): TemporalGraph
  def until(endTime: String): TemporalGraph
  def slice(startTime: Long, endTime: Long): TemporalGraph
  def slice(startTime: String, endTime: String): TemporalGraph
  def raphtorize(increment: Long): RaphtoryGraph
  def raphtorize(increment: String): RaphtoryGraph
  def raphtorize(increment: Long, window: Long): RaphtoryGraph
  def raphtorize(increment: String, window: String): RaphtoryGraph
  def raphtorize(increment: Long, windows: List[Long]): RaphtoryGraph
  def raphtorize(increment: String, windows: List[String]): RaphtoryGraph
}
