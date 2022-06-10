package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.visitor.ExplodedVertex
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.api.time.Interval
import com.raphtory.api.time.NullInterval
import com.raphtory.internals.components.querymanager.PointPath
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.SinglePoint
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.time.DateTimeParser
import com.typesafe.config.Config
import com.raphtory.internals.time.IntervalParser.{parse => parseInterval}

private[api] trait TemporalGraphBase[G <: TemporalGraphBase[G, FixedG], FixedG <: FixedGraph[
        FixedG
]] extends GraphBase[G, TemporalGraph, MultilayerTemporalGraph] {
  private[api] val query: Query
  private[api] val querySender: QuerySender
  private[api] val conf: Config

  /** Creates a new `TemporalGraph` which includes all activity after startTime (inclusive).
    * @param startTime time interpreted in milliseconds by default
    */
  def from(startTime: Long): G = {
    val updatedStart = query.timelineStart max startTime
    newGraph(query.copy(timelineStart = updatedStart), querySender)
  }

  /** Creates a new `TemporalGraph` which includes all activity after startTime (inclusive). */
  def from(startTime: String): G = from(parseDateTime(startTime))

  /** Creates a new `TemporalGraph` which includes all activity before endTime (inclusive).
    * @param endTime time interpreted in milliseconds by default
    */
  def to(endTime: Long): G = {
    val updatedEnd = query.timelineEnd min endTime
    newGraph(query.copy(timelineEnd = updatedEnd), querySender)
  }

  /** Creates a new `TemporalGraph` which includes all activity before endTime (inclusive). */
  def to(endTime: String): G = to(parseDateTime(endTime))

  /** Creates a new `TemporalGraph` which includes all activity before endTime (exclusive).
    * @param endTime time interpreted in milliseconds by default
    */
  def until(endTime: Long): G = to(endTime - 1)

  /** Creates a new `TemporalGraph` which includes all activity before endTime (exclusive). */
  def until(endTime: String): G = until(parseDateTime(endTime))

  /** Creates a new `TemporalGraph` which includes all activity between `startTime` (inclusive) and `endTime` (exclusive)
    *  `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)`
    */
  def slice(startTime: Long, endTime: Long): G = this from startTime until endTime

  /** Creates a new `TemporalGraph` which includes all activity between `startTime` (inclusive) and `endTime` (exclusive)
    * `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)`.
    */
  def slice(startTime: String, endTime: String): G =
    slice(parseDateTime(startTime), parseDateTime(endTime))

  /** Create a `DottedGraph` with a temporal mark at `time`.
    * @param time the temporal mark to be added to the timeline
    */
  def at(time: Long): DottedGraph[FixedG] =
    new DottedGraph(newFixedGraph(query.copy(points = SinglePoint(time)), querySender))

  /** Create a `DottedGraph` with a temporal mark at `time`.
    * @param time the temporal mark to be added to the timeline
    */
  def at(time: String): DottedGraph[FixedG] = at(parseDateTime(time))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the
    * timeline aligned with 0.
    * @param increment the step size
    */
  def walk(increment: Long): DottedGraph[FixedG] = setPointPath(DiscreteInterval(increment))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
    * covering all the timeline aligned with `offset`.
    * @param increment the step size
    * @param offset the offset to align with
    */
  def walk(increment: Long, offset: Long): DottedGraph[FixedG] =
    setPointPath(DiscreteInterval(increment), offset = DiscreteInterval(offset))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the
    * timeline aligned with 0.
    * @param increment the step size
    */
  def walk(increment: String): DottedGraph[FixedG] = setPointPath(parseInterval(increment))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the timeline aligned with `offset`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param increment the interval to use as the step size
    * @param offset the interval to expressing the offset from the epoch to align with
    */
  def walk(increment: String, offset: String): DottedGraph[FixedG] =
    setPointPath(parseInterval(increment), offset = parseInterval(offset))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param start the point to create the first temporal mark
    * @param increment the step size
    */
  def depart(start: Long, increment: Long): DottedGraph[FixedG] =
    setPointPath(DiscreteInterval(increment), start = Some(start))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param start the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size
    */
  def depart(start: String, increment: String): DottedGraph[FixedG] =
    setPointPath(parseInterval(increment), start = Some(parseDateTime(start)))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` from the start of the timeline ending at `end`.
    * @param end the point to create the last temporal mark
    * @param increment the step size
    */
  def climb(end: Long, increment: Long): DottedGraph[FixedG] =
    setPointPath(DiscreteInterval(increment), end = Some(end))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * @param start the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size
    */
  def climb(end: String, increment: String): DottedGraph[FixedG] =
    setPointPath(parseInterval(increment), end = Some(parseDateTime(end)))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start` and ending at `end` (with a smaller step at the end if necessary).
    * @param start the point to create the first temporal mark
    * @param end the point to create the last temporal mark
    * @param increment the step size
    */
  def range(start: Long, end: Long, increment: Long): DottedGraph[FixedG] =
    setPointPath(DiscreteInterval(increment), start = Some(start), end = Some(end))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start` and ending at `end` (with a smaller step at the end if necessary).
    * @param start the timestamp to create the first temporal mark
    * @param end the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size
    */
  def range(start: String, end: String, increment: String): DottedGraph[FixedG] =
    setPointPath(
            parseInterval(increment),
            start = Some(parseDateTime(start)),
            end = Some(parseDateTime(end))
    )

  private def setPointPath(
      increment: Interval,
      start: Option[Long] = None,
      end: Option[Long] = None,
      offset: Interval = NullInterval
  ) =
    new DottedGraph(
            newFixedGraph(
                    query.copy(points = PointPath(increment, start, end, offset)),
                    querySender
            )
    )

  private def parseDateTime(dateTime: String) =
    DateTimeParser(conf.getString("raphtory.query.timeFormat")).parse(dateTime)

  override private[api] def newRGraph(query: Query, querySender: QuerySender): TemporalGraph =
    new TemporalGraph(query, querySender, conf)

  override private[api] def newMGraph(
      query: Query,
      querySender: QuerySender
  ): MultilayerTemporalGraph =
    new MultilayerTemporalGraph(query, querySender, conf)

  private[api] def newFixedGraph(query: Query, querySender: QuerySender): FixedG
}

/** A graph with an underlying timeline with a start time and, optionally, an end time.
  *
  * It offers methods to modify this timeline.
  * There are also methods to create a one or a sequence of temporal marks over the timeline,
  * therefore producing a so-called DottedGraph,
  * that can be further used to create a set of perspectives over the timeline of the graph to work with.
  * This class supports all the graph operations defined in `GraphOperations`.
  * If any graph operation is invoked from this instance, it is applied only over the elements of the graph within
  * the timeline.
  *
  * @note All the timestamps must follow the format set in the configuration path `"raphtory.query.timeFormat"`.
  *  By default is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`
  *  All the strings expressing intervals need to be in the format `"<number> <unit> [<number> <unit> [...]]"`,
  *  where numbers must be integers and units must be one of
  *  {'year', 'month', 'week', 'day', 'hour', 'min'/'minute', 'sec'/'second', 'milli'/'millisecond'}
  *  using the plural when the number is different than 1.
  *  Commas and the construction 'and' are omitted to allow natural text.
  *  For instance, the interval "1 month 1 week 3 days" can be rewritten as "1 month, 1 week, and 3 days"
  *
  * @see [[DottedGraph]], [[GraphPerspective]]
  */
class TemporalGraph private[api] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    override private[api] val conf: Config
) extends TemporalGraphBase[TemporalGraph, RaphtoryGraph]
        with ReducedGraphViewImplementation[TemporalGraph, MultilayerTemporalGraph] {

  override def newFixedGraph(query: Query, querySender: QuerySender): RaphtoryGraph =
    new RaphtoryGraph(query, querySender)
}

/** The multilayer view corresponding to [[TemporalGraph]]
  *
  * This exposes the same timeline operations as [[TemporalGraph]] but extends
  * [[MultilayerGraphView]] which means that all algorithm operations act on [[visitor.ExplodedVertex]]s.
  *
  * @see [[TemporalGraph]], [[MultilayerGraphView]], [[DottedGraph]]
  */
class MultilayerTemporalGraph private[api] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    override private[api] val conf: Config
) extends TemporalGraphBase[MultilayerTemporalGraph, MultilayerRaphtoryGraph]
        with MultilayerGraphViewImplementation[MultilayerTemporalGraph, TemporalGraph] {

  override def newFixedGraph(query: Query, querySender: QuerySender): MultilayerRaphtoryGraph =
    new MultilayerRaphtoryGraph(query, querySender)
}
