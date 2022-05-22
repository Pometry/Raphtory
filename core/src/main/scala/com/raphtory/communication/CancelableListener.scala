package com.raphtory.communication

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
          try listener.close()
          catch {
            case e: NullPointerException => //already Nulled by the closer of the scheduler
          }
        })
    }
}
