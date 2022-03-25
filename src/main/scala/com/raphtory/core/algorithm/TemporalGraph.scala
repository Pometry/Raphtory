package com.raphtory.core.algorithm

import com.raphtory.core.client.QuerySender
import com.raphtory.core.components.querymanager.PointPath
import com.raphtory.core.components.querymanager.Query
import com.raphtory.core.components.querymanager.SinglePoint
import com.raphtory.core.time.DateTimeParser
import com.raphtory.core.time.DiscreteInterval
import com.raphtory.core.time.Interval
import com.raphtory.core.time.NullInterval
import com.raphtory.core.time.IntervalParser.{parse => parseInterval}
import com.typesafe.config.Config

/**
  * {s}`TemporalGraph`
  *  : Public interface for the analysis API
  *
  * A {s}`TemporalGraph` is a {s}`RaphtoryGraph` with only one view. Because of this, there is a unique underlying
  * timeline with a start time and, optionally, an end time. It offers methods to modify this timeline.
  * There are also methods to create a collection of views over it, transforming this graph into a {s}`RaphtoryGraph`.
  * If any graph operation is invoked from this instance, it is applied only over the elements of the graph within
  * the timeline.
  *
  * ## Methods
  *
  *  {s}`from(startTime: Long): TemporalGraph`
  *    : Set the start of the timeline to {s}`startTime`.
  *
  *      {s}`startTime: Long`
  *      : time interpreted in milliseconds by default
  *
  *  {s}`from(startTime: String): TemporalGraph`
  *    : Set the start of the timeline to {s}`startTime`. The format of the timestamp can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`.
  *
  *      {s}`startTime: String`
  *      : timestamp
  *
  *  {s}`until(endTime: Long): TemporalGraph`
  *    : Set the end of the timeline to {s}`endTime`.
  *
  *      {s}`endTime: Long`
  *        : time interpreted in milliseconds by default
  *
  *  {s}`until(endTime: String): TemporalGraph`
  *    : Set the end of the timeline to {s}`endTime`. The format of the timestamp can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`.
  *
  *      {s}`endTime: String`
  *        : timestamp
  *
  *  {s}`slice(startTime: Long, endTime: Long): TemporalGraph`
  *    : Set the start and the end of the timeline to {s}`startTime` and {s}`endTime` respectively.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`
  *
  *  {s}`slice(startTime: String, endTime: String): TemporalGraph`
  *     : Set the start and the end of the timeline to {s}`startTime` and {s}`endTime` respectively.
  *    {s}`graph.slice(startTime, endTime)` is equivalent to {s}`graph.from(startTime).until(endTime)`.
  *    The format of the timestamps can be set in the configuration
  *    path: {s}`"raphtory.query.timeFormat"`. By default is {s}`"yyyy-MM-dd[ HH:mm:ss[.SSS]]"`.
  *
  *   {s}`raphtorize(increment: Long): RaphtoryGraph`
  *     : Create a collection of incrementally growing views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *  {s}`raphtorize(increment: String): RaphtoryGraph`
  *     : Create a collection of incrementally growing views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *         : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *  {s}`raphtorize(increment: Long, window: Long): RaphtoryGraph`
  *     : Create a collection of equally sized views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *       {s}`window: Long`
  *         : the window size
  *
  *  {s}`raphtorize(increment: String, window: String): RaphtoryGraph`
  *     : Create a collection of equally sized views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *          : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *       {s}`window: String`
  *         : the window size in natural language
  *
  *  {s}`raphtorize(increment: Long, windows: List[Long]): RaphtoryGraph`
  *    : Create a collection of views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment` and for every window size set in {s}`windows`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: Long`
  *         : the step size
  *
  *       {s}`windows: Long`
  *         : the window sizes
  *
  *  {s}`raphtorize(increment: String, windows: List[String]): RaphtoryGraph`
  *     : Create a collection of views over the graph beginning from the start of the
  *     timeline with the step size set by {s}`increment` and for every window size set in {s}`windows`.
  *     If the timeline has an end, the last view is created at that point.
  *     If not, it produces an unbounded collection of views.
  *
  *       {s}`increment: String`
  *          : the step size in natural language. E.g. {s}`"2 seconds"`, {s}`"1 months"`, {s}`"1 day and 12 hours"`
  *
  *       {s}`windows: String`
  *         : the window sizes in natural language
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.RaphtoryGraph)
  * ```
  */
class TemporalGraph(query: Query, private val querySender: QuerySender, private val conf: Config)
        extends RaphtoryGraph(query, querySender) {

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

  def slice(startTime: String, endTime: String): TemporalGraph = this from startTime until endTime

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
