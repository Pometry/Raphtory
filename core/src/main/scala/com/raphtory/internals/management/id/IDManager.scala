package com.raphtory.internals.management.id

private[raphtory] trait IDManager {
  def getNextAvailableID(): Option[Int]
  def stop(): Unit
}
