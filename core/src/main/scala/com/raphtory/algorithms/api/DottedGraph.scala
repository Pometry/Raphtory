package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.Interval
import com.raphtory.time.IntervalParser.{parse => parseInterval}
import com.typesafe.config.Config

object Alignment extends Enumeration {
  type Alignment = Value
  val START, MIDDLE, END = Value
}

/**
  * {s}`DottedGraph`
  *  : Public interface for the analysis API
  *
  * A {s}`DottedGraph` is a {s}`TemporalGraph` with one or a sequence of temporal marks over the timeline
  * that can be used as references to create perspectives of the graph.
  * To do so, this class offers methods to look to the past or to the future from every temporal mark
  * or to create windows that end ({s}`Alignment.END`),
  * have its center ({s}`Alignment.MIDDLE`),
  * or start ({s}`Alignment.START`) at every temporal mark.
  *
  * ```{note}
  * When creating a window from a temporal mark, the mark is always included within the window.
  * Particularly, for the three alignment options we have, the bounds of the windows are as follows:
  *  - {s}`Alignment.START`: The start of the window is inclusive and the end exclusive
  *  - {s}`Alignment.MIDDLE`: The start of the window is inclusive and the end exclusive
  *  - {s}`Alignment.END`: The start of the window is exclusive and the end inclusive
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
  *  {s}`window(size: Long): RaphtoryGraph`
  *    : Create a window with the given {s}`size` starting from every temporal mark.
  *
  *      {s}`size: Long`
  *      : the exact size of the window
  *
  *  {s}`window(size: Long, alignment: Alignment.Value): RaphtoryGraph`
  *    : Create a window with the given {s}`size` and the given {s}`alignment`
  *    using every temporal mark.
  *
  *      {s}`size: Long`
  *      : the exact size of the window
  *
  *      {s}`alignment: Alignment.Value`
  *      : the alignment of the window
  *
  *  {s}`window(size: String): RaphtoryGraph`
  *    : Create a window with the given {s}`size` starting from every temporal mark.
  *
  *      {s}`size: Long`
  *      : interval expressing the exact size of the window
  *
  *  {s}`window(size: String, alignment: Alignment.Value): RaphtoryGraph`
  *    : Create a window with the given {s}`size` and the given {s}`alignment`
  *    using every temporal mark.
  *
  *      {s}`size: Long`
  *      : interval expressing the exact size of the window
  *
  *      {s}`alignment: Alignment.Value`
  *      : the alignment of the window
  *
  *  {s}`window(sizes: List[Long]): RaphtoryGraph`
  *    : Create a number of windows with the given {s}`sizes` starting from every temporal mark.
  *
  *      {s}`sizes: List[Long]`
  *      : the exact sizes of the windows
  *
  *  {s}`window(sizes: List[Long], alignment: Alignment.Value): RaphtoryGraph`
  *    : Create a number of windows with the given {s}`sizes` and the given {s}`alignment`
  *    using every temporal mark.
  *
  *      {s}`sizes: List[Long]`
  *      : the exact sizes of the windows
  *
  *      {s}`alignment: Alignment.Value`
  *      : the alignment of the windows
  *
  *  {s}`window(sizes: => List[String]): RaphtoryGraph`
  *    : Create a number of windows with the given {s}`sizes` starting from every temporal mark.
  *
  *      {s}`sizes: => List[String]`
  *      : intervals expressing the exact sizes of the windows
  *
  *  {s}`window(sizes: => List[String], alignment: Alignment.Value): RaphtoryGraph`
  *    : Create a number of windows with the given {s}`sizes` and the given {s}`alignment`
  *    using every temporal mark.
  *
  *      {s}`sizes: => List[String]`
  *      : intervals expressing the exact sizes of the windows
  *
  *      {s}`alignment: Alignment.Value`
  *      : the alignment of the windows
  *
  * ```{seealso}
  * [](com.raphtory.core.algorithm.RaphtoryGraph)
  * ```
  */
private[raphtory] class DottedGraph(
    query: Query,
    private val querySender: QuerySender,
    private val conf: Config
) {

  def window(size: Long): RaphtoryGraph =
    window(size, Alignment.START)

  def window(size: Long, alignment: Alignment.Value): RaphtoryGraph =
    addWindows(List(DiscreteInterval(size)), alignment)

  def window(size: String): RaphtoryGraph = window(size, Alignment.START)

  def window(size: String, alignment: Alignment.Value): RaphtoryGraph =
    addWindows(List(parseInterval(size)), alignment)

  def window(sizes: List[Long]): RaphtoryGraph =
    window(sizes, Alignment.START)

  def window(sizes: List[Long], alignment: Alignment.Value): RaphtoryGraph =
    addWindows(sizes map DiscreteInterval, alignment)

  def window(sizes: => List[String]): RaphtoryGraph =
    window(sizes, Alignment.START)

  def window(sizes: => List[String], alignment: Alignment.Value): RaphtoryGraph =
    addWindows(sizes map parseInterval, alignment)

  def past(): RaphtoryGraph = addWindows(List(), Alignment.END)

  def future(): RaphtoryGraph = addWindows(List(), Alignment.START)

  private def addWindows(sizes: List[Interval], alignment: Alignment.Value = Alignment.START) =
    new RaphtoryGraph(
            query.copy(windows = sizes, windowAlignment = alignment),
            querySender
    )
}
