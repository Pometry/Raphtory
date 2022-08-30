package com.raphtory.internals.graph

class SourceTracker(sourceID: Int) {

  private var totalSent        = Int.MaxValue
  private var receivedMessages = 0
  private var blocking         = true

  def block(): Unit               = blocking = true
  def unblock(): Unit             = blocking = false
  def receiveMessage: Unit        = receivedMessages += 1
  def setTotalSent(newTotal: Int) = totalSent = newTotal
  def isBlocking                  = blocking || receivedMessages < totalSent

}

object SourceTracker {
  def apply(sourceID: Int) = new SourceTracker(sourceID)
}
