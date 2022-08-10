package com.raphtory.internals.communication

sealed private[raphtory] trait Topic[+T] {
  def id: String
  def subTopic: String
  def customAddress: String
  def connector: Connector
}

sealed private[raphtory] trait CanonicalTopic[+T] extends Topic[T] {
  def endPoint[E >: T]: EndPoint[E] = connector.endPoint(0, this)   // TODO Fix srcParId ??
}

private[raphtory] case class ExclusiveTopic[T](
    connector: Connector,
    id: String,
    subTopic: String = "",
    customAddress: String = ""
) extends CanonicalTopic[T]

private[raphtory] case class WorkPullTopic[T](
    connector: Connector,
    id: String,
    subTopic: String = "",
    customAddress: String = ""
) extends CanonicalTopic[T]

private[raphtory] case class BroadcastTopic[T](
    numListeners: Int,
    connector: Connector,
    id: String,
    subTopic: String = "",
    customAddress: String = ""
) extends CanonicalTopic[T]

private[raphtory] case class ShardingTopic[T](
    numPartitions: Int,
    connector: Connector,
    id: String,
    subTopic: String = "",
    customAddress: String = ""
) extends Topic[T] {

  def endPoint(srcParId: Int): Map[Int, EndPoint[T]] = {
    val partitions = 0 until numPartitions
    val endPoints  = partitions map { partition =>
      val topic = exclusiveTopicForPartition(partition)
      (partition, connector.endPoint[T](srcParId, topic))
    }
    endPoints.toMap
  }

  def exclusiveTopicForPartition(partition: Int): ExclusiveTopic[T] =
    ExclusiveTopic[T](connector, id, s"$subTopic-$partition")
}
