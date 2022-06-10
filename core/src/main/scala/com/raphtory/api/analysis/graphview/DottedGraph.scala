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
  * A DottedGraph is a [[TemporalGraph]] with one or a sequence of temporal marks over the timeline
  * that can be used as references to create perspectives of the graph.
  * To do so, this class offers methods to look to the past or to the future from every temporal mark
  * or to create windows that end ([[Alignment.END]]),
  * have its center ([[Alignment.MIDDLE]]),
  * or start ([[Alignment.START]]) at every temporal mark.
  *
  * When creating a window from a temporal mark, the mark is always included within the window.
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

  /**  Creates a window from the given size starting via every temporal mark
    *  The start of the window is inclusive and the end exclusive
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def window(size: Long): G =
    window(size, Alignment.START)

  /** Creates a window from the given size and alignment starting from every temporal mark
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def window(size: Long, alignment: Alignment.Value): G =
    addWindows(List(DiscreteInterval(size)), alignment)

  /**  Creates a window from the given size starting via every temporal mark.
    *  The start of the window is inclusive and the end exclusive.
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def window(size: String): G = window(size, Alignment.START)

  /** Creates a window from the given size and alignment starting from every temporal mark.
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def window(size: String, alignment: Alignment.Value): G =
    addWindows(List(parseInterval(size)), alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[Int]): G =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[Int], alignment: Alignment.Value): G =
    addWindows(sizes map (DiscreteInterval(_)), alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[Long])(implicit ignore: DummyImplicit): G =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[Long], alignment: Alignment.Value)(implicit
      ignore: DummyImplicit
  ): G =
    addWindows(sizes map DiscreteInterval, alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[String])(implicit ignore: ClassTag[String]): G =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[String], alignment: Alignment.Value)(implicit
      ignore: ClassTag[String]
  ): G =
    addWindows(sizes map parseInterval, alignment)

  /** For each temporal mark create a window starting from the current point and including all older edges
    */
  def past(): G = addWindows(List(), Alignment.END)

  /** For each temporal mark create a window starting from the current mark and including all newer edges
    */
  def future(): G = addWindows(List(), Alignment.START)

  private def addWindows(sizes: List[Interval], alignment: Alignment.Value = Alignment.START) =
    graph.newGraph(
            graph.query.copy(windows = sizes, windowAlignment = alignment),
            graph.querySender
    )
}
