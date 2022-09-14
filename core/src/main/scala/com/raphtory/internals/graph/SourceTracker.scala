package com.raphtory.internals.graph

import scala.collection.mutable

class SourceTracker(sourceID: Long) {

  private var totalSent        = Long.MaxValue
  private val receivedMessages = mutable.Map[Int, Long]()

  private var lastPercent = 0.0

  def setReceivedMessage(partitionID: Int, newReceivedMessages: Long): Unit =
    receivedMessages.put(partitionID, newReceivedMessages)
  def setTotalSent(newTotal: Long): Unit                                    = totalSent = newTotal

  def whatPercent(): Option[String] = {

    if (lastPercent == 100) return None

    val percent = Math.floor(receivedMessages.values.sum.toDouble / totalSent * 100)
    if (percent > lastPercent) {
      lastPercent = percent
      if (percent == 100)
        Some(s"Source $sourceID has completed ingesting and will now unblock")
      else
        Some(s"Source $sourceID currently ingested $percent% of its updates.")
    }
    else None

  }

  def isBlocking: Boolean =
    receivedMessages.values.sum < totalSent

}

object SourceTracker {
  def apply(sourceID: Long) = new SourceTracker(sourceID)
}
