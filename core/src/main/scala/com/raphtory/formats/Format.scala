package com.raphtory.formats

import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector

trait Format[T] {
  def open(connector: SinkConnector[T]): Unit
  def setupPerspective(connector: SinkConnector[T], perspective: Perspective): Unit
  def writeRow(connector: SinkConnector[T], row: Row): Unit
  def closePerspective(connector: SinkConnector[T]): Unit
  def close(connector: SinkConnector[T]): Unit
}
