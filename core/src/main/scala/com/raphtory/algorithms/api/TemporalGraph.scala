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
  * {s}`TemporalGraph`
  *  : Public interface for the analysis API
  *
  * A {s}`TemporalGraph` is a graph with an underlying
  * timeline with a start time and, optionally, an end time. It offers methods to modify this timeline.
  * There are also methods to create a one or a sequence of temporal marks over the timeline,
  * therefore producing a so-called DottedGraph,
  * that can be further used to create a set of perspectives over the timeline of the graph to work with.
  * This class supports all the graph operations defined in {s}`GraphOperations`.
  * If any graph operation is invoked from this instance, it is applied only over the elements of the graph within
  * the timeline.
  *
  * ```{note}
  *  All the timestamps must follow the format set in the configuration path {s}`"raphtory.query.timeFormat"`.
  *  By default is {s}`"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`.
  *
  *  All the strings expressing intervals need to be in the format {s}`"<number> <unit> [<number> <unit> [...]]"`,
  *  where numbers must be integers and units must be one of
  *  {'year', 'month', 'week', 'day', 'hour', 'min'/'minute', 'sec'/'second', 'milli'/'millisecond'}
  *  using the plural when the number is different than 1.
  *  Commas and the construction 'and' are omitted to allow natural text.
  *  For instance, the interval "1 month 1 week 3 days" can be rewritten as "1 month, 1 week, and 3 days"
  * ```
  *
  * ## Methods
  *
  *  {s}`from(startTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity before {s}`startTime`.
  *
  *      {s}`startTime: Long`
  *      : time interpreted in milliseconds by default
  *
  *  {s}`from(startTime: String): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity before {s}`startTime`.
  *
  *      {s}`startTime: String`
  *      : timestamp
  *
  *  {s}`until(endTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after {s}`endTime`.
  *
  *      {s}`endTime: Long`
  *        : time interpreted in milliseconds by default
  *
  *  {s}`until(endTime: String): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the activity after {s}`endTime`.
  *
  *      {s}`endTime: String`
  *        : timestamp
  *
  *  {s}`slice(startTime: Long, endTime: Long): TemporalGraph`
  *    : Creates a new `TemporalGraph` filtering out all the before {s}`startTime` and after {s}`endTime`.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`
  *
  *  {s}`slice(startTime: String, endTime: String): TemporalGraph`
  *     : Creates a new `TemporalGraph` filtering out all the before {s}`startTime` and after {s}`endTime`.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`.
  *
  *      {s}`startTime: String`
  *        : timestamp
  *
  *      {s}`endTime: String`
  *        : timestamp
  *
  *   {s}`at(time: Long): DottedGraph`
  *     : Create a {s}`DottedGraph` with a temporal mark at {s}`time`.
  *
  *       {s}`time: Long`
  *         : the temporal mark to be added to the timeline
  *
  *  {s}`at(time: String): DottedGraph`
  *     : Create a {s}`DottedGraph` with a temporal mark at {s}`time`.
  *
  *       {s}`time: String`
  *         : timestamp
  *
  *  {s}`walk(increment: Long): DottedGraph`
  *     : Create a {s}`DottedGraph` with a sequence of temporal marks with a separation of {s}`increment`
  *     covering all the timeline aligned with 0.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`walk(increment: Long, offset: Long): DottedGraph`
  *     : Create a {s}`DottedGraph` with a sequence of temporal marks with a separation of {s}`increment`
  *     covering all the timeline aligned with {s}`offset`.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *       {s}`offset: Long`
  *         : the offset to align with
  *
  *  {s}`walk(increment: String): DottedGraph`
  *     : Create a {s}`DottedGraph` with a sequence of temporal marks with a separation of {s}`increment`
  *     covering all the timeline aligned with the epoch.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       {s}`increment: String`
  *         : the interval to use as the step size
  *
  *  {s}`walk(increment: String, offset: String): DottedGraph`
  *     : Create a {s}`DottedGraph` with a sequence of temporal marks with a separation of {s}`increment`
  *     covering all the timeline aligned with {s}`offset`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       {s}`increment: String`
  *         : the interval to use as the step size
  *
  *       {s}`offset: String`
  *         : the interval to expressing the offset from the epoch to align with
  *
  *  {s}`depart(start: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     starting at {s}`start`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       {s}`start: Long`
  *         : the point to create the first temporal mark
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`depart(start: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     starting at {s}`start`.
  *     These temporal marks get generated as the timeline keeps growing.
  *
  *       {s}`start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       {s}`increment: String`
  *         : the interval expressing the step size
  *
  *  {s}`climb(end: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     from the start of the timeline ending at {s}`end`.
  *
  *       {s}`end: Long`
  *         : the point to create the last temporal mark
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`climb(end: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     starting at {s}`start`.
  *
  *       {s}`start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       {s}`increment: String`
  *         : the interval expressing the step size
  *
  *  {s}`range(start: Long, end: Long, increment: Long): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     starting at {s}`start` and ending at {s}`end` (with a smaller step at the end if necessary).
  *
  *       {s}`start: Long`
  *         : the point to create the first temporal mark
  *
  *       {s}`end: Long`
  *         : the point to create the last temporal mark
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`range(start: String, end: String, increment: String): DottedGraph`
  *     : Create a DottedGraph with a sequence of temporal marks with a separation of {s}`increment`
  *     starting at {s}`start` and ending at {s}`end` (with a smaller step at the end if necessary).
  *
  *       {s}`start: String`
  *         : the timestamp to create the first temporal mark
  *
  *       {s}`end: String`
  *         : the timestamp to create the first temporal mark
  *
  *       {s}`increment: String`
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

  def until(endTime: Long): TemporalGraph = {
    val updatedEnd = query.timelineEnd min endTime
    new TemporalGraph(query.copy(timelineEnd = updatedEnd), querySender, conf)
  }

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
    setPointPath(DiscreteInterval(increment), start = start, customStart = true)

  def depart(start: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), start = parseDateTime(start), customStart = true)

  def climb(end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), end = end)

  def climb(end: String, increment: String): DottedGraph =
    setPointPath(parseInterval(increment), end = parseDateTime(end))

  def range(start: Long, end: Long, increment: Long): DottedGraph =
    setPointPath(DiscreteInterval(increment), start = start, end = end, customStart = true)

  def range(start: String, end: String, increment: String): DottedGraph =
    setPointPath(
            parseInterval(increment),
            start = parseDateTime(start),
            end = parseDateTime(end),
            customStart = true
    )

  private def setPointPath(
      increment: Interval,
      start: Long = Long.MinValue,
      end: Long = Long.MaxValue,
      offset: Interval = NullInterval,
      customStart: Boolean = false
  ) = {
    assert(customStart || (!customStart && start == Long.MinValue))
    new DottedGraph(
            query.copy(points = PointPath(increment, start, end, offset, customStart)),
            querySender,
            conf
    )
  }

  private def parseDateTime(dateTime: String) =
    DateTimeParser(conf.getString("raphtory.query.timeFormat")).parse(dateTime)
}
