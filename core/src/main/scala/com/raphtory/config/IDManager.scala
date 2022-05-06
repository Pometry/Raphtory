package com.raphtory.config

trait IDManager {
  def getNextAvailableID(): Option[Int]
  def resetID(): Unit
  def stop(): Unit
}
