package com.raphtory.graph.visitor

case class InterlayerEdge(
    sourceTime: Long,
    dstTime: Long,
    properties: Map[String, Any] = Map.empty[String, Any]
) extends EntityVisitor {

  override def Type(): String =
    properties.get("type") match {
      case Some(value) => value.toString
      case None        => ""
    }

  override def firstActivityAfter(time: Long): HistoricEvent = ???

  override def lastActivityBefore(time: Long): HistoricEvent = ???

  override def getPropertySet(): List[String] = properties.keys.toList

  override def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[(Long, T)]] =
    properties.get(key) match {
      case None        => None
      case Some(value) =>
        val actualTime = Seq(sourceTime, dstTime, after).min
        if (actualTime > before)
          Some(List.empty[(Long, T)])
        else
          Some(List[(Long, T)]((actualTime, value.asInstanceOf[T])))
    }

  override def latestActivity(): HistoricEvent =
    HistoricEvent(math.max(sourceTime, dstTime), event = false)

  override def earliestActivity(): HistoricEvent =
    HistoricEvent(math.min(sourceTime, dstTime), event = true)

  override def getPropertyAt[T](key: String, time: Long): Option[T] =
    if (time < math.min(sourceTime, dstTime) || time > math.max(sourceTime, dstTime))
      None
    else
      properties.get(key).map(_.asInstanceOf[T])

  override def history(): List[HistoricEvent] = List(earliestActivity(), latestActivity())

  override def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean =
    after < earliestActivity().time && latestActivity().time < before

  override def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean =
    time < latestActivity().time && time > earliestActivity().time
}
