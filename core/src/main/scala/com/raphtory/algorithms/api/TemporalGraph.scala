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

/**  Public interface for the Temporal Graph analysis API
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
  * @note All the timestamps must follow the format set in the configuration path `"raphtory.query.timeFormat"`.
  *  By default is `"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`
  *  All the strings expressing intervals need to be in the format `"<number> <unit> [<number> <unit> [...]]"`,
  *  where numbers must be integers and units must be one of
  *  {'year', 'month', 'week', 'day', 'hour', 'min'/'minute', 'sec'/'second', 'milli'/'millisecond'}
  *  using the plural when the number is different than 1.
  *  Commas and the construction 'and' are omitted to allow natural text.
  *  For instance, the interval "1 month 1 week 3 days" can be rewritten as "1 month, 1 week, and 3 days"
  *
  * @see [[com.raphtory.algorithms.api.DottedGraph]], [[com.raphtory.algorithms.api.GraphOperations]]
  */
class TemporalGraph(
    query: Query,
    private val querySender: QuerySender,
    private val conf: Config
) extends RaphtoryGraph(query, querySender) {

  /** Creates a new `TemporalGraph` which includes all activity after startTime (inclusive).
    * @param startTime time interpreted in milliseconds by default
    *  */
  def from(startTime: Long): TemporalGraph = {
    val updatedStart = query.timelineStart max startTime
    new TemporalGraph(query.copy(timelineStart = updatedStart), querySender, conf)
  }

  /** Creates a new `TemporalGraph` which includes all activity after startTime (inclusive). */
  def from(startTime: String): TemporalGraph = from(parseDateTime(startTime))

  /** Creates a new `TemporalGraph` which includes all activity before endTime (inclusive).
    * @param endTime time interpreted in milliseconds by default
    */
  def to(endTime: Long): TemporalGraph = {
    val updatedEnd = query.timelineEnd min endTime
    new TemporalGraph(query.copy(timelineEnd = updatedEnd), querySender, conf)
  }

  /** Creates a new `TemporalGraph` which includes all activity before endTime (inclusive). */
  def to(endTime: String): TemporalGraph = to(parseDateTime(endTime))

  /** Creates a new `TemporalGraph` which includes all activity before endTime (exclusive).
    * @param endTime time interpreted in milliseconds by default */
  def until(endTime: Long): TemporalGraph = to(endTime - 1)

  /** Creates a new `TemporalGraph` which includes all activity before endTime (exclusive). */
  def until(endTime: String): TemporalGraph = until(parseDateTime(endTime))

  /** Creates a new `TemporalGraph` which includes all activity between `startTime` (inclusive) and `endTime` (exclusive)
    *  `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)` */
  def slice(startTime: Long, endTime: Long): TemporalGraph = this from startTime until endTime

  /** Creates a new `TemporalGraph` which includes all activity between `startTime` (inclusive) and `endTime` (exclusive)
    * `graph.slice(startTime, endTime)` is equivalent to `graph.from(startTime).until(endTime)`. */
  def slice(startTime: String, endTime: String): TemporalGraph =
    slice(parseDateTime(startTime), parseDateTime(endTime))

  /** Create a `DottedGraph` with a temporal mark at `time`.
    * @param time the temporal mark to be added to the timeline */
  def at(time: Long): DottedGraph =
    new DottedGraph(query.copy(points = SinglePoint(time)), querySender, conf)

  /** Create a `DottedGraph` with a temporal mark at `time`.
    * @param time the temporal mark to be added to the timeline */
  def at(time: String): DottedGraph = at(parseDateTime(time))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the
    * timeline aligned with 0.
    * @param increment the step size
    * */
  def walk(increment: Long): DottedGraph = setPointPath(DiscreteInterval(increment))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment`
    * covering all the timeline aligned with `offset`.
    * @param increment the step size
    * @param offset the offset to align with */
  def walk(increment: Long, offset: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), offset = DiscreteInterval(offset))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the
    * timeline aligned with 0.
    * @param increment the step size */
  def walk(increment: String): DottedGraph = setPointPath(parseInterval(increment))

  /** Create a `DottedGraph` with a sequence of temporal marks with a separation of `increment` covering all the timeline aligned with `offset`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param increment the interval to use as the step size
    * @param offset the interval to expressing the offset from the epoch to align with
    * */
  def walk(increment: String, offset: String): DottedGraph =
    setPointPath(parseInterval(increment), offset = parseInterval(offset))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param start the point to create the first temporal mark
    * @param increment the step size
    * */
  def depart(start: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), start = Some(start))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * These temporal marks get generated as the timeline keeps growing.
    * @param start the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size
    * */
  def depart(start: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), start = Some(parseDateTime(start)))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` from the start of the timeline ending at `end`.
    * @param end the point to create the last temporal mark
    * @param increment the step size
    * */
  def climb(end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), end = Some(end))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start`.
    * @param start the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size
    * */
  def climb(end: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), end = Some(parseDateTime(end)))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start` and ending at `end` (with a smaller step at the end if necessary).
    * @param start the point to create the first temporal mark
    * @param end the point to create the last temporal mark
    * @param increment the step size
    * */
  def range(start: Long, end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), start = Some(start), end = Some(end))

  /** Create a DottedGraph with a sequence of temporal marks with a separation of `increment` starting at `start` and ending at `end` (with a smaller step at the end if necessary).
    * @param start the timestamp to create the first temporal mark
    * @param end the timestamp to create the first temporal mark
    * @param increment the interval expressing the step size */
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
