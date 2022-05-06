package com.raphtory.config

import java.util.concurrent.CompletableFuture

/** @DoNotDocument */
trait EndPoint[T] {
  def sendAsync(message: T): Unit
  def flushAsync(): CompletableFuture[Void]
  def close(): Unit
  def closeWithMessage(message: T): Unit
}
