package com.raphtory.core.algorithm

import com.raphtory.core.client.QueryBuilder
import com.raphtory.core.time.DateTimeParser
import com.raphtory.core.time.DiscreteInterval
import com.raphtory.core.time.Interval
import com.raphtory.core.time.IntervalParser.{parse => parseInterval}
import com.typesafe.config.Config

/**
  * @DoNotDocument
  */
class DefaultTemporalGraph(queryBuilder: QueryBuilder, private val conf: Config)
        extends DefaultRaphtoryGraph(queryBuilder)
        with TemporalGraph {

  override def from(startTime: Long): TemporalGraph =
    new DefaultTemporalGraph(queryBuilder.setStartTime(startTime), conf)

  override def from(startTime: String): TemporalGraph = {
    println(conf.getString("raphtory.query.timeFormat"))
    from(DateTimeParser(conf.getString("raphtory.query.timeFormat")).parse(startTime))
  }

  override def until(endTime: Long): TemporalGraph =
    new DefaultTemporalGraph(queryBuilder.setEndTime(endTime), conf)

  override def until(endTime: String): TemporalGraph =
    until(DateTimeParser(conf.getString("raphtory.query.timeFormat")).parse(endTime))

  override def slice(startTime: Long, endTime: Long): TemporalGraph =
    this from startTime until endTime

  override def slice(startTime: String, endTime: String): TemporalGraph =
    this from startTime until endTime

  override def raphtorize(increment: Long): RaphtoryGraph = raphtorize(increment, List())

  override def raphtorize(increment: String): RaphtoryGraph =
    raphtorize(increment, List())

  override def raphtorize(increment: Long, window: Long): RaphtoryGraph =
    raphtorize(increment, List(window))

  override def raphtorize(increment: String, window: String): RaphtoryGraph =
    raphtorize(increment, List(window))

  override def raphtorize(increment: Long, windows: List[Long]): RaphtoryGraph =
    raphtorize(Some(DiscreteInterval(increment)), windows map DiscreteInterval)

  override def raphtorize(increment: String, windows: List[String]): RaphtoryGraph =
    raphtorize(Some(parseInterval(increment)), windows map parseInterval)

  private def raphtorize(increment: Option[Interval], windows: List[Interval]) = {
    val queryBuilderWithIncrement = increment match {
      case Some(increment) => queryBuilder.setIncrement(increment)
      case None            => queryBuilder
    }
    new DefaultRaphtoryGraph(queryBuilderWithIncrement.setWindows(windows))
  }
}
