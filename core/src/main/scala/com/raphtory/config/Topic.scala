package com.raphtory.config

sealed trait Topic[+T] {
  def id: String
  def subTopic: String
  def connector: Connector
}

sealed trait CanonicalTopic[+T] extends Topic[T] {
  def endPoint[E >: T]: EndPoint[E] = connector.endPoint(this)
}

case class ExclusiveTopic[+T](
    connector: Connector,
    id: String,
    subTopic: String = ""
) extends CanonicalTopic[T]

case class WorkPullTopic[T](
    connector: Connector,
    id: String,
    subTopic: String = ""
) extends CanonicalTopic[T]

case class BroadcastTopic[T](
    connector: Connector,
    id: String,
    subTopic: String = ""
) extends CanonicalTopic[T]

case class ShardingTopic[T](
    numPartitions: Int,
    connector: Connector,
    id: String,
    subTopic: String = ""
) extends Topic[T] {

  def endPoint: Map[Int, EndPoint[T]] = {
    val partitions = 0 until numPartitions
    val endPoints  = partitions map { partition =>
      val topic = exclusiveTopicForPartition(partition)
      (partition, connector.endPoint[T](topic))
    }
    endPoints.toMap
  }

  def exclusiveTopicForPartition(partition: Int): ExclusiveTopic[T] =
    ExclusiveTopic[T](connector, id, s"$subTopic-$partition")
}
