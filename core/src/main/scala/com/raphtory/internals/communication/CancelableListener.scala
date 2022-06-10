package com.raphtory.internals.communication

private[raphtory] trait CancelableListener {
  def start(): Unit
  def close(): Unit
}

private[raphtory] object CancelableListener {

  def apply(listeners: Seq[CancelableListener]): CancelableListener =
    new CancelableListener {
      override def start(): Unit = listeners foreach (_.start())

      override def close(): Unit =
        listeners foreach (listener => {
          listener.close()
        })
    }
}
