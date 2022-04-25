package com.raphtory.config

sealed trait Topic[T] {
  type MsgType = T
  def baseAddress: String
  def suffix: String
  def connector: Connector

  def address(jobId: Option[String], partition: Option[Int]): String = {
    val jobIdSuffix     = if (jobId.isDefined) s"-$jobId" else ""
    val partitionSuffix = if (partition.isDefined) s"-$partition" else ""
    s"$baseAddress$jobIdSuffix$partitionSuffix"
  }
}

sealed trait CanonicalTopic[T] extends Topic[T] {
  def endPoint: EndPoint[T] = connector.endPoint(this)
}

case class ExclusiveTopic[T](connector: Connector, baseAddress: String, suffix: String = "")
        extends CanonicalTopic[T] {
  def register(messageHandler: T => Unit): Unit = connector.listen(messageHandler, this)
}

case class WorkPullTopic[T](connector: Connector, baseAddress: String, suffix: String = "")
        extends CanonicalTopic[T] {
  def register(messageHandler: T => Unit): Unit = connector.listen(messageHandler, this)
}

case class BroadcastTopic[T](connector: Connector, baseAddress: String, suffix: String = "")
        extends CanonicalTopic[T] {

  def register(messageHandler: T => Unit, partition: Int): Unit =
    connector.listen(messageHandler, this, partition)
}

case class ShardingTopic[T](
    numPartitions: Int,
    connector: Connector,
    baseAddress: String,
    suffix: String = ""
) extends Topic[T] {

  def register(messageHandler: T => Unit, partition: Int): Unit =
    topicForPartition(partition).register(messageHandler)

  def endPoint(sharding: T => Int): EndPoint[T] =
    new EndPoint[T] {
      private val partitions = 0 until numPartitions

      private val endPoints = partitions map { partition =>
        val topic = topicForPartition(partition)
        connector.endPoint[T](topic)
      }

      override def sendAsync(message: T): Unit =
        endPoints(sharding.apply(message)) sendAsync message
      override def close(): Unit               = endPoints foreach (_.close())

      override def closeWithMessage(message: T): Unit =
        endPoints foreach (_.closeWithMessage(message))
    }

  private def topicForPartition(partition: Int) =
    ExclusiveTopic[T](connector, baseAddress, s"$suffix-$partition")
}
