package com.raphtory.internal.communication

/** @DoNotDocument */
trait CancelableListener {
  def start(): Unit
  def close(): Unit
}

object CancelableListener {

  def apply(listeners: Seq[CancelableListener]): CancelableListener =
    new CancelableListener {
      override def start(): Unit = listeners foreach (_.start())

      override def close(): Unit =
        listeners foreach (listener => {
          listener.close()
        })
    }
}
