package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.PointPath
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querymanager.SinglePoint
import com.raphtory.time.DateTimeParser
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.Interval
import com.raphtory.time.NullInterval
import com.raphtory.time.IntervalParser.{parse => parseInterval}
import com.typesafe.config.Config

/**
  * `TemporalGraph`
  *  : Public interface for the analysis API
  *
  * A `TemporalGraph` is a graph with an underlying
  * timeline with a start time and, optionally, an end time. It offers methods to modify this timeline.
  * There are also methods to create a one or a sequence of temporal marks over the timeline,
  * therefore producing a so-called DottedGraph,
  * that can be further used to create a set of perspectives over the timeline of the graph to work with.
  * This class supports all the graph operations defined in `GraphOperations`.
  * If any graph operation is invoked from this instance, it is applied only over the elements of the graph within
  * the timeline.
  *
  * ```{note}
  *  All the timestamps must follow the format set in the configuration path `"raphtory.query.timeFormat"`.
  *  By default is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`.
  *
  *  All the strings expressing intervals need to be in the format `"<number> <unit> [<number> <unit> [...]]"`,
  *  where numbers must be integers and units must be one of
  *  {'year', 'month', 'week', 'day', 'hour', 'min'/'minute', 'sec'/'second', 'milli'/'millisecond'}
  *  using the plural when the number is different than 1.
  *  Commas and the construction 'and' are omitted to allow natural text.
  *  For instance, the interval "1 month 1 week 3 days" can be rewritten as "1 month, 1 week, and 3 days"
  * ```
  *
  * ## Methods
  *
  *  `from(startTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity before `startTime`.
  *
  *      `startTime: Long`
  *      : time interpreted in milliseconds by default
  *
  *  `from(startTime: String): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity before `startTime`.
  *
  *      `startTime: String`
  *      : timestamp
  *
  *  `to(endTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after `endTime`.
  *
  *      `endTime: Long`
  *        : time interpreted in milliseconds by default
  *
  *  `to(endTime: String): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after `endTime`.
  *
  *      `endTime: String`
  *        : timestamp
  *
  *  `until(endTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after and at `endTime`.
  *
  *      `endTime: Long`
  *        : time interpreted in milliseconds by default
  *
  *  `until(endTime: String): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after and at `endTime`.
  *
  *      `endTime: String`
  *        : timestamp
  *
  *  `slice(startTime: Long, endTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity
  *    before `startTime` and after and at `endTime`.
  *    `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)`
  *
  *  `slice(startTime: String, endTime: String): TemporalGraph`
  *     : Creates a new `TemporalGraph` filtering out all the activity
  *     before `startTime` and after and at `endTime`.
  *    `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)`.
  *
  *      `startTime: String`
  *        : timestamp
  *
  *      `endTime: String`
  *        : timestamp
  *
  *   `at(time: Long): DottedGraph`
  *     : Create a `DottedGraph` with a temporal mark at `time`.
  *
  *       `time: Long`
  *         : the temporal mark to be added to the timeline
  *
  *  `at(time: String): DottedGraph`
  *     : Create a `DottedGraph` with a temporal mark at `time`.
  *
  *       `time: String`
  *         : timestamp
  *
  *  `walk(increment: Long): DottedGraph`
  *     : Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
  *     covering all the timeline aligned with 0.
  *
  *       `increment: Long`
  *         : the step size
  *
  *  `walk(increment: Long, offset: Long): DottedGraph`
  *     : Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
  *     covering all the timeline aligned with `offset`.
  *
  *       `increment: Long`
  *         : the step size
  *
  *       `offset: Long`
  *         : the offset to align with
  *
  *  `walk(increment: String): DottedGraph`
  *     : Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
  *     covering all the timeline aligned with the epoch.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       `increment: String`
  *         : the interval to use as the step size
  *
  *  `walk(increment: String, offset: String): DottedGraph`
  *     : Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
  *     covering all the timeline aligned with `offset`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       `increment: String`
  *         : the interval to use as the step size
  *
  *       `offset: String`
  *         : the interval to expressing the offset from the epoch to align with
  *
  *  `depart(start: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     starting at `start`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       `start: Long`
  *         : the point to create the first temporal mark
  *
  *       `increment: Long`
  *         : the step size
  *
  *  `depart(start: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     starting at `start`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       `start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       `increment: String`
  *         : the interval expressing the step size
  *
  *  `climb(end: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     from the start of the timeline ending at `end`.
  *
  *       `end: Long`
  *         : the point to create the last temporal mark
  *
  *       `increment: Long`
  *         : the step size
  *
  *  `climb(end: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     starting at `start`.
  *
  *       `start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       `increment: String`
  *         : the interval expressing the step size
  *
  *  `range(start: Long, end: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     starting at `start` and ending at `end` (with a smaller step at the end if necessary).
  *
  *       `start: Long`
  *         : the point to create the first temporal mark
  *
  *       `end: Long`
  *         : the point to create the last temporal mark
  *
  *       `increment: Long`
  *         : the step size
  *
  *  `range(start: String, end: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of `increment`
  *     starting at `start` and ending at `end` (with a smaller step at the end if necessary).
  *
  *       `start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       `end: String`
  *         : the timestamp to create the first temporal mark
  *
  *       `increment: String`
  *         : the interval expressing the step size
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.DottedGraph), [](com.raphtory.algorithms.api.GraphOperations)
  * ```
  */
private[raphtory] class TemporalGraph(
    query: Query,
    private val querySender: QuerySender,
    private val conf: Config
) extends RaphtoryGraph(query, querySender) {

  def from(startTime: Long): TemporalGraph = {
    val updatedStart = query.timelineStart max startTime
    new TemporalGraph(query.copy(timelineStart = updatedStart), querySender, conf)
  }

  def from(startTime: String): TemporalGraph = from(parseDateTime(startTime))

  def to(endTime: Long): TemporalGraph = {
    val updatedEnd = query.timelineEnd min endTime
    new TemporalGraph(query.copy(timelineEnd = updatedEnd), querySender, conf)
  }

  def to(endTime: String): TemporalGraph = to(parseDateTime(endTime))

  def until(endTime: Long): TemporalGraph = to(endTime - 1)

  def until(endTime: String): TemporalGraph = until(parseDateTime(endTime))

  def slice(startTime: Long, endTime: Long): TemporalGraph = this from startTime until endTime

  def slice(startTime: String, endTime: String): TemporalGraph =
    slice(parseDateTime(startTime), parseDateTime(endTime))

  def at(time: Long): DottedGraph =
    new DottedGraph(query.copy(points = SinglePoint(time)), querySender, conf)

  def at(time: String): DottedGraph = at(parseDateTime(time))

  def walk(increment: Long): DottedGraph = setPointPath(DiscreteInterval(increment))

  def walk(increment: Long, offset: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), offset = DiscreteInterval(offset))

  def walk(increment: String): DottedGraph = setPointPath(parseInterval(increment))

  def walk(increment: String, offset: String): DottedGraph =
    setPointPath(parseInterval(increment), offset = parseInterval(offset))

  def depart(start: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), start = Some(start))

  def depart(start: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), start = Some(parseDateTime(start)))

  def climb(end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), end = Some(end))

  def climb(end: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), end = Some(parseDateTime(end)))

  def range(start: Long, end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), start = Some(start), end = Some(end))

  def range(start: String, end: String, increment: String): DottedGraph =
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
            query.copy(points = PointPath(increment, start, end, offset)),
            querySender,
            conf
    )

  private def parseDateTime(dateTime: String) =
    DateTimeParser(conf.getString("raphtory.query.timeFormat")).parse(dateTime)
}
