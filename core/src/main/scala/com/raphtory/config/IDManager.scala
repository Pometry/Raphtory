package com.raphtory.config

/** @DoNotDocument */
trait IDManager {
  def getNextAvailableID(): Option[Int]
  def resetID(): Unit
  def stop(): Unit
}
