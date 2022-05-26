package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.Interval
import com.raphtory.time.IntervalParser.{parse => parseInterval}
import com.typesafe.config.Config

import scala.reflect.ClassTag

object Alignment extends Enumeration {
  type Alignment = Value
  val START, MIDDLE, END = Value
}

/** Public interface for the analysis API
  *
  * A DottedGraph is a TemporalGraph with one or a sequence of temporal marks over the timeline
  * that can be used as references to create perspectives of the graph.
  * To do so, this class offers methods to look to the past or to the future from every temporal mark
  * or to create windows that end (Alignment.END),
  * have its center (Alignment.MIDDLE),
  * or start (Alignment.START) at every temporal mark.
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
  *  @see  [[com.raphtory.algorithms.api.RaphtoryGraph]]
  */
private[raphtory] class DottedGraph(
    query: Query,
    private val querySender: QuerySender,
    private val conf: Config
) {

  implicit val ignoreInt: Int       = 1
  implicit val ignoreString: String = ""

  /**  Creates a window from the given size starting via every temporal mark
    *  The start of the window is inclusive and the end exclusive
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def window(size: Long): RaphtoryGraph =
    window(size, Alignment.START)

  /** Creates a window from the given size and alignment starting from every temporal mark
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def window(size: Long, alignment: Alignment.Value): RaphtoryGraph =
    addWindows(List(DiscreteInterval(size)), alignment)

  /**  Creates a window from the given size starting via every temporal mark.
    *  The start of the window is inclusive and the end exclusive.
    *
    *  @param size the exact size of the window
    *  @return A modified Raphtory graph with the window size
    */
  def window(size: String): RaphtoryGraph = window(size, Alignment.START)

  /** Creates a window from the given size and alignment starting from every temporal mark.
    *
    *  @param size the exact size of the window
    *  @param alignment the alignment of the window
    *  @return A modified Raphtory graph with the window size and alignment
    */
  def window(size: String, alignment: Alignment.Value): RaphtoryGraph =
    addWindows(List(parseInterval(size)), alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[Int]): RaphtoryGraph =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[Int], alignment: Alignment.Value): RaphtoryGraph =
    addWindows(sizes map (DiscreteInterval(_)), alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[Long])(implicit ignore: DummyImplicit): RaphtoryGraph =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[Long], alignment: Alignment.Value)(implicit
      ignore: DummyImplicit
  ): RaphtoryGraph =
    addWindows(sizes map DiscreteInterval, alignment)

  /** Create a number of windows with the given sizes starting from every temporal mark.
    * The start of the window is inclusive and the end exclusive.
    *
    * @param sizes the exact sizes of the windows
    * @return A modified Raphtory graph with the window sizes
    */
  def window(sizes: List[String])(implicit ignore: ClassTag[String]): RaphtoryGraph =
    window(sizes, Alignment.START)

  /** Create a number of windows with the given sizes and given alignment starting from every temporal mark.
    *
    * @param sizes the exact sizes of the windows
    * @param alignment the alignment of the windows
    * @return A modified Raphtory graph with the window sizes and given alignment
    */
  def window(sizes: List[String], alignment: Alignment.Value)(implicit
      ignore: ClassTag[String]
  ): RaphtoryGraph =
    addWindows(sizes map parseInterval, alignment)

  /**
    */
  def past(): RaphtoryGraph = addWindows(List(), Alignment.END)

  /**
    */
  def future(): RaphtoryGraph = addWindows(List(), Alignment.START)

  /**
    */
  private def addWindows(sizes: List[Interval], alignment: Alignment.Value = Alignment.START) =
    new RaphtoryGraph(
            query.copy(windows = sizes, windowAlignment = alignment),
            querySender
    )
}
