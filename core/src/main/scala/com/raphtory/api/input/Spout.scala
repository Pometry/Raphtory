package com.raphtory.api.input

import com.raphtory.internals.graph.GraphBuilder
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Base trait for spouts.
  *
  * The spout is a trait which is used to define a process to get data into Raphtory from an external source. This is based on the iterator
  * interface where the user has to define the `hasNext` and `next` functions which specify if there is data and if so
  * how it can be retrieved.
  *
  * Spouts have a generic T argument allowing them to output any type of data as long as an equivalent function
  * is available to parse each data tuple into updates.
  *
  * There are a variety of Spout implementations within Raphtory including for Files, Streams and Cloud Services.
  * To minimise the size of the core Raphtory jar these can be viewed in the Connectors package.
  *
  * @see [[com.raphtory.spouts.FileSpout FileSpout]], [[com.raphtory.Raphtory]], [[Graph]]
  */
trait Spout[T] {
  def buildSpout(): SpoutInstance[T]
}

trait SpoutInstance[T] extends Iterator[T] {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def hasNextIterator(): Boolean  = hasNext
  def nextIterator(): Iterator[T] = this

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}
}
