package com.raphtory.api.analysis.graphview

import com.raphtory.api.time.DiscreteInterval
import com.raphtory.api.time.Interval
import com.raphtory.internals.time.IntervalParser.{parse => parseInterval}

import scala.reflect.ClassTag

/** Alignment types for windows */
object Alignment extends Enumeration {
  type Alignment = Value
  val START, MIDDLE, END = Value
}

/** Marked timeline view of a [[TemporalGraph]]
  *
  * A DottedGraph is a [[TemporalGraph]] with one or a sequence of temporal epochs across the timeline
  * that can be used as references to create perspectives of the graph.
  * To do so, this class offers methods to look to the past or to the future from every temporal epoch
  * or to create windows that end ([[Alignment.END]]),
  * have its center ([[Alignment.MIDDLE]]),
  * or start ([[Alignment.START]]) at every temporal epoch.
  *
  * When creating a window from a temporal epoch, they are always included within the window.
  * Particularly, for the three alignment options we have, the bounds of the windows are as follows:
  *  - Alignment.START: The start of the window is inclusive and the end exclusive
  *  - Alignment.MIDDLE: The start of the window is inclusive and the end exclusive
  *  - Alignment.END: The start of the window is exclusive and the end inclusive
  *
  *  All the strings expressing intervals need to be in the format <number> <unit> ,
  *  where numbers must be integers and units must be one of
  *  {'year', 'month', 'week', 'day', 'hour', 'min'/'minute', 'sec'/'second', 'milli'/'millisecond'}
  *  using the plural when the number is different than 1.
  *  Commas and the construction 'and' are omitted to allow natural text.
  *  For instance, the interval "1 month 1 week 3 days" can be rewritten as "1 month, 1 week, and 3 days"
  *  @see  [[RaphtoryGraph]], [[TemporalGraph]]
  */
class DottedGraph[G <: FixedGraph[G]] private[api] (
    graph: G
) {

  implicit val ignoreInt: Int       = 1
  implicit val ignoreString: String = ""

  //****** LONG BASED WINDOWING ***********

  /**  Creates a window from the given size starting via every temporal epoch
    *  By default the window looks into the past (Alignment.END)
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def window(size: Long): G =
    window(size, Alignment.END)

  /** Creates a window from the given size and alignment from every temporal epoch
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def window(size: Long, alignment: Alignment.Value): G =
    addWindows(Array(DiscreteInterval(size)), alignment)

  /** Create a number of windows with the given sizes at every temporal epoch.
    * By default the window looks into the past (Alignment.END)
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: Iterable[Long]): G =
    window(sizes, Alignment.END)

  /** Create a number of windows with the given sizes and given alignment at every temporal epoch.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: Iterable[Long], alignment: Alignment.Value): G =
    addWindows(sizes.map(DiscreteInterval).toArray, alignment)

  //****** STRING BASED WINDOWING ***********

  /**  Creates a window from the given size at every temporal epoch.
    *  By default the window looks into the past (Alignment.END)
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def dateWindow(size: String): G = dateWindow(size, Alignment.END)

  /** Creates a window from the given size and alignment at every temporal epoch.
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def dateWindow(size: String, alignment: Alignment.Value): G =
    addWindows(Array(parseInterval(size)), alignment)

  /** Create a number of windows with the given sizes at every temporal epoch.
    * By default the window looks into the past (Alignment.END)
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def dateWindow(sizes: Iterable[String]): G =
    dateWindow(sizes, Alignment.END)

  /** Create a number of windows with the given sizes and given alignment at every temporal epoch.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def dateWindow(sizes: Iterable[String], alignment: Alignment.Value): G =
    addWindows(sizes.map(x => parseInterval(x, graph.query.datetimeQuery)).toArray, alignment)

  /** For each temporal epoch create a window starting from the current point and include all older events
    */
  def past(): G = addWindows(Array(), Alignment.END)

  /** For each temporal epoch create a window starting from the current point and include all newer events
    */
  def future(): G = addWindows(Array(), Alignment.START)

  private def addWindows(sizes: Array[Interval], alignment: Alignment.Value = Alignment.END) =
    graph.newGraph(
            graph.query.copy(windows = sizes, windowAlignment = alignment),
            graph.querySender
    )
}
