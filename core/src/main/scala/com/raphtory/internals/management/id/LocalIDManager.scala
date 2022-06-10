package com.raphtory.internals.management.id

import java.util.concurrent.atomic.AtomicInteger

private[raphtory] class LocalIDManager extends IDManager {
  private val nextId = new AtomicInteger(0)

  override def getNextAvailableID(): Option[Int] = Some(nextId.getAndIncrement())
  override def resetID(): Unit                   = nextId.set(0)
  override def stop(): Unit = {}
}
