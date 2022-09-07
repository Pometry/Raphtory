package com.raphtory.internals.communication

import java.util.concurrent.CompletableFuture

private[raphtory] trait EndPoint[T] {
  def sendAsync(message: T): Unit
  def flushAsync(): CompletableFuture[Void]

  def flushAndSendAsync(message: T): CompletableFuture[Unit] =
    flushAsync().thenApply(_ => sendAsync(message))
  def close(): Unit
  def closeWithMessage(message: T): Unit
}
