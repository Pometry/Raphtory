package com.raphtory.internals.components

import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.management.Scheduler
import com.typesafe.scalalogging.Logger
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

trait FlushToFlight {

  val logger: Logger
  val scheduler: Scheduler
  val writers: Map[Int, EndPoint[_]]

  def close(): Unit = {
    logger.trace("Shutting Down: FlushToFlight")
    flushTask.foreach(cancelable => cancelable())
    writers.values.foreach(_.flushAsync())
  }

  @volatile private var flushed                                  = false
  @volatile private var nTimes                                   = 0
  @volatile private var lastFlushedTime                          = -1L
  @volatile private[components] var latestMsgTimeToFlushToFlight = -1L
  private var flushTask: Option[() => Future[Unit]]              = None

  // Flush data to flight from the time no graph updates have been seen
  def flushArrow(): Unit = {
    if (lastFlushedTime == latestMsgTimeToFlushToFlight && lastFlushedTime != -1L) {
      if (!flushed) {
        nTimes += 1
        if (nTimes == 4) {
            writers.values.foreach(_.flushAsync())
            flushed = true
          nTimes = 0
        }
      }
    }
    else flushed = false
    lastFlushedTime = latestMsgTimeToFlushToFlight

    runFlushArrow()
  }

  def runFlushArrow(): Unit = flushTask = Option(scheduler.scheduleOnce(1.seconds, flushArrow()))

  runFlushArrow()
}
