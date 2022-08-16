package com.raphtory.internals.communication

private[raphtory] trait Connector {

  def register[T](
      partitionId: Int,
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener

  def endPoint[T](srcParId: Int, topic: CanonicalTopic[T]): EndPoint[T]

}
