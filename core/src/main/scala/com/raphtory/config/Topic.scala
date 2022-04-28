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

  def endPoint(sharding: T => Int): EndPoint[T] =
    new EndPoint[T] {
      private val partitions = 0 until numPartitions

      private val endPoints = partitions map { partition =>
        val topic = exclusiveTopicForPartition(partition)
        connector.endPoint[T](topic)
      }

      override def sendAsync(message: T): Unit =
        endPoints(sharding.apply(message)) sendAsync message
      override def close(): Unit               = endPoints foreach (_.close())

      override def closeWithMessage(message: T): Unit =
        endPoints foreach (_.closeWithMessage(message))
    }

  def exclusiveTopicForPartition(partition: Int): ExclusiveTopic[T] =
    ExclusiveTopic[T](connector, id, s"$subTopic-$partition")
}
