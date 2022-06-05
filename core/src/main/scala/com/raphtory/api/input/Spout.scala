package com.raphtory.api.input

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

trait Spout[T] extends Iterator[T] {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def hasNextIterator(): Boolean  = hasNext
  def nextIterator(): Iterator[T] = this

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}
}
