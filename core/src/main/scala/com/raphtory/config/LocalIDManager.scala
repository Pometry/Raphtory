package com.raphtory.config

/** @DoNotDocument */
class LocalIDManager extends IDManager {
  private var nextId = 0

  override def getNextAvailableID(): Option[Int] = {
    val id = nextId
    nextId += 1
    Some(id)
  }

  override def resetID(): Unit = nextId = 0

  override def stop(): Unit = {}
}
