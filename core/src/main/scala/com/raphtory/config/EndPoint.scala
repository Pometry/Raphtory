package com.raphtory.config

trait EndPoint[T] {
  def sendAsync(message: T): Unit
  def close(): Unit
  def closeWithMessage(message: T): Unit
}
