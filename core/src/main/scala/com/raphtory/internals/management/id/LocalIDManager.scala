package com.raphtory.internals.management.id

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

private[raphtory] class LocalIDManager extends IDManager {
  private val IDTrackers = mutable.Map[String, AtomicInteger]()

  override def getNextAvailableID(graphID: String): Option[Int] =
    IDTrackers.get(graphID) match {
      case Some(tracker) => Some(tracker.getAndIncrement())
      case None          =>
        val tracker = new AtomicInteger(0)
        IDTrackers.put(graphID, tracker)
        Some(tracker.getAndIncrement())
    }
}
