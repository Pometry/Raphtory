package com.raphtory.internals.graph

import scala.collection.mutable

class SourceTracker() {

  private var totalSent        = Long.MaxValue
  private val receivedMessages = mutable.Map[Int, Long]()
  private var blocking         = true

  def block(): Unit   = blocking = true
  def unblock(): Unit = blocking = false

  def setReceivedMessage(partitionID: Int, newReceivedMessages: Long): Unit =
    receivedMessages.put(partitionID, newReceivedMessages)
  def setTotalSent(newTotal: Long): Unit                                    = totalSent = newTotal
  def isBlocking: Boolean                                                   = blocking || receivedMessages.values.sum < totalSent

}

object SourceTracker {
  def apply() = new SourceTracker()
}
