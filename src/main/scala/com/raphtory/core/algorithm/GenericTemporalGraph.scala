package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.time.DateTimeParser
import com.raphtory.core.time.DiscreteInterval
import com.raphtory.core.time.Interval
import com.raphtory.core.time.TimeUtils.parseInterval
import com.typesafe.config.Config

class GenericTemporalGraph(private val queryBuilder: QueryBuilder, private val conf: Config)
        extends GenericGraphPerspectiveSet(queryBuilder)
        with TemporalGraph {

  override def from(startTime: Long): TemporalGraph =
    new GenericTemporalGraph(queryBuilder.setStartTime(startTime), conf)

  override def from(startTime: String): TemporalGraph = from(DateTimeParser(conf).parse(startTime))

  override def until(endTime: Long): TemporalGraph =
    new GenericTemporalGraph(queryBuilder.setEndTime(endTime), conf)

  override def until(endTime: String): TemporalGraph = until(DateTimeParser(conf).parse(endTime))

  override def slice(startTime: Long, endTime: Long): TemporalGraph =
    this from startTime until endTime

  override def slice(startTime: String, endTime: String): TemporalGraph =
    this from startTime until endTime

  override def raphtorize(increment: Long): GraphPerspectiveSet = raphtorize(increment, List())

  override def raphtorize(increment: String): GraphPerspectiveSet =
    raphtorize(increment, List())

  override def raphtorize(increment: Long, window: Long): GraphPerspectiveSet =
    raphtorize(increment, List(window))

  override def raphtorize(increment: String, window: String): GraphPerspectiveSet =
    raphtorize(increment, List(window))

  override def raphtorize(increment: Long, windows: List[Long]): GraphPerspectiveSet =
    raphtorize(Some(DiscreteInterval(increment)), windows map DiscreteInterval)

  override def raphtorize(increment: String, windows: List[String]): GraphPerspectiveSet =
    raphtorize(Some(parseInterval(increment)), windows map parseInterval)

  private def raphtorize(increment: Option[Interval], windows: List[Interval]) = {
    val queryBuilderWithIncrement = increment match {
      case Some(increment) => queryBuilder.setIncrement(increment)
      case None            => queryBuilder
    }
    new GenericGraphPerspectiveSet(queryBuilderWithIncrement.setWindows(windows))
  }
}
