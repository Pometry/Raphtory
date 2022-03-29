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

class DottedGraph(query: Query, private val querySender: QuerySender, private val conf: Config) {

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
