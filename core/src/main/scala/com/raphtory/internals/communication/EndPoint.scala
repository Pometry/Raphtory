package com.raphtory.internals.communication

import java.util.concurrent.CompletableFuture

private[raphtory] trait EndPoint[T] {

  /** send message without waiting (messages may be send out of order) */
  def sendAsync(message: T): Unit

  /** flush all messages in the queue */
  def flushAsync(): CompletableFuture[Void]

  /** flush all messages in the queue and send message after
    * (the new message is guaranteed to be send after all current messages, however, messages sent asynchronously after
    * this call may be sent before or after this message)
    */
  def flushAndSendAsync(message: T): CompletableFuture[Unit] =
    flushAsync().thenApply(_ => sendAsync(message))

  /** send message after flushing all messages in the queue and wait for it to be sent */
  def sendSync(message: T): Unit

  /** close endpoint and clean up */
  def close(): Unit

  /** send message just before closing the endpoint */
  def closeWithMessage(message: T): Unit
}
