package com.raphtory.internals.graph

import scala.collection.mutable

class SourceTracker() {

  private var totalSent        = Long.MaxValue
  private val receivedMessages = mutable.Map[Int, Long]()

  def setReceivedMessage(partitionID: Int, newReceivedMessages: Long): Unit =
    receivedMessages.put(partitionID, newReceivedMessages)
  def setTotalSent(newTotal: Long): Unit                                    = totalSent = newTotal
  def isBlocking: Boolean                                                   = receivedMessages.values.sum < totalSent

}

object SourceTracker {
  def apply() = new SourceTracker()
}
