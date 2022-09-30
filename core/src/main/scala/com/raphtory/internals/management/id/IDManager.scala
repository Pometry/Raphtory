package com.raphtory.internals.management.id

private[raphtory] trait IDManager {
  def getNextAvailableID(graphID: String): Option[Int]
}
