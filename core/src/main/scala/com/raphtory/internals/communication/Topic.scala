package com.raphtory.internals.communication

sealed private[raphtory] trait Topic[+T] {
  def id: String
  def subTopic: String
  def customAddress: String
  def connector: Connector
}

sealed private[raphtory] trait CanonicalTopic[+T] extends Topic[T] {
  def endPoint[E >: T]: EndPoint[E] = connector.endPoint(this)
}

private[raphtory] case class ExclusiveTopic[T](
    connector: Connector,
    id: String,
    subTopic: String = "",
    customAddress: String = ""
) extends CanonicalTopic[T] {

  override def toString: String =
    if (subTopic.nonEmpty)
      if (customAddress.nonEmpty)
        s"$id/$subTopic/$customAddress"
      else
        s"$id/$subTopic"
    else if (customAddress.nonEmpty)
      s"$id/$customAddress"
    else
      id

}

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

  def endPoint(): Map[Int, EndPoint[T]] = {
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
