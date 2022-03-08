package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.time.DiscreteInterval

class GenericTemporalGraph(private val queryBuilder: QueryBuilder)
        extends GenericGraphPerspectiveSet(queryBuilder)
        with TemporalGraph {

  def from(startTime: Long): TemporalGraph =
    new GenericTemporalGraph(queryBuilder.setStartTime(startTime))

  def until(endTime: Long): TemporalGraph =
    new GenericTemporalGraph(queryBuilder.setEndTime(endTime))

  def slice(startTime: Long, endTime: Long): TemporalGraph = this from startTime until endTime

  def raphtorize(increment: Long): GraphPerspectiveSet = raphtorize(increment, List())

  def raphtorize(increment: Long, window: Long): GraphPerspectiveSet =
    raphtorize(increment, List(window))

  def raphtorize(increment: Long, windows: List[Long]): GraphPerspectiveSet =
    new GenericGraphPerspectiveSet(
            queryBuilder
              .setIncrement(DiscreteInterval(increment))
              .setWindows(windows map DiscreteInterval)
    )
}
