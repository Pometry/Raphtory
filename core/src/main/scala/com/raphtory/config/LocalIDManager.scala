package com.raphtory.config

import monix.execution.atomic.AtomicInt

/** @DoNotDocument */
class LocalIDManager extends IDManager {
  private val nextId = AtomicInt(0)

  override def getNextAvailableID(): Option[Int] = Some(nextId.getAndIncrement())
  override def resetID(): Unit                   = nextId.set(0)
  override def stop(): Unit = {}
}
