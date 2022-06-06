package com.raphtory.internals.management.id

/** @DoNotDocument */
trait IDManager {
  def getNextAvailableID(): Option[Int]
  def resetID(): Unit
  def stop(): Unit
}
