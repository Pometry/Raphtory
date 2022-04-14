package com.raphtory.graph.visitor

/**
  * {s}`InterlayerEdge(srcTime: Long, dstTime: Long, properties: Map[String, Any] = Map.empty[String, Any])`
  *   : Class for representing interlayer edges
  *
  *     {s}`srcTime: Long`
  *       : source time-stamp for the edge
  *
  *     {s}`dstTime: Long`
  *       : destination time-stamp for the edge
  *
  *     {s}`properties: Map[String, Any] = Map.empty[String, Any]`
  *       : properties that the interlayer edge should have
  */
case class InterlayerEdge(
    srcTime: Long,
    dstTime: Long,
    properties: Map[String, Any] = Map.empty[String, Any]
) extends EntityVisitor {

  override def Type(): String =
    properties.get("type") match {
      case Some(value) => value.toString
      case None        => ""
    }

  override def firstActivityAfter(time: Long): Option[HistoricEvent] = ???

  override def lastActivityBefore(time: Long): Option[HistoricEvent] = ???

  override def getPropertySet(): List[String] = properties.keys.toList

  override def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[(Long, T)]] =
    properties.get(key) match {
      case None        => None
      case Some(value) =>
        val actualTime = Seq(srcTime, dstTime, after).min
        if (actualTime > before)
          Some(List.empty[(Long, T)])
        else
          Some(List[(Long, T)]((actualTime, value.asInstanceOf[T])))
    }

  override def latestActivity(): HistoricEvent =
    HistoricEvent(math.max(srcTime, dstTime), event = false)

  override def earliestActivity(): HistoricEvent =
    HistoricEvent(math.min(srcTime, dstTime), event = true)

  override def getPropertyAt[T](key: String, time: Long): Option[T] =
    if (time < math.min(srcTime, dstTime) || time > math.max(srcTime, dstTime))
      None
    else
      properties.get(key).map(_.asInstanceOf[T])

  override def history(): List[HistoricEvent] = List(earliestActivity(), latestActivity())

  override def active(after: Long = Long.MinValue, before: Long = Long.MaxValue): Boolean =
    after < earliestActivity().time && latestActivity().time < before

  override def aliveAt(time: Long, window: Long = Long.MaxValue): Boolean =
    time < latestActivity().time && time > earliestActivity().time
}

/** # InterlayerEdgeBuilders
  *
  *  {s}`InterlayerEdgeBuilders`
  *   : Default builders for constructing interlayer edges
  *
  *  ## Builders
  *
  *  {s}`linkNext`
  *
  *  {s}`linkPrevious`
  *
  *  {s}`linkPreviousAndNext`
  */
object InterlayerEdgeBuilders {

  def linkNext(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkNext((_, _) => properties)

  def linkNext(
      propertyBuilder: (Long, Long) => Map[String, Any] = (_, _) => Map.empty[String, Any]
  ): Vertex => Seq[InterlayerEdge] = { vertex =>
    vertex
      .history()
      .sliding(2)
      .collect {
        case List(event1, event2) if event1.event && event2.event =>
          InterlayerEdge(event1.time, event2.time, propertyBuilder(event1.time, event2.time))
      }
      .toSeq
  }

  def linkPrevious(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkPrevious((_, _) => properties)

  def linkPrevious(
      propertyBuilder: (Long, Long) => Map[String, Any] = (_, _) => Map.empty[String, Any]
  ): Vertex => Seq[InterlayerEdge] = { vertex =>
    vertex
      .history()
      .sliding(2)
      .collect {
        case List(event1, event2) if event1.event && event2.event =>
          InterlayerEdge(event2.time, event1.time, propertyBuilder(event2.time, event1.time))
      }
      .toSeq
  }

  def linkPreviousAndNext(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkPreviousAndNext((_, _) => properties)

  def linkPreviousAndNext(
      propertyBuilder: (Long, Long) => Map[String, Any] = (_, _) => Map.empty[String, Any]
  ): Vertex => Seq[InterlayerEdge] = { vertex =>
    vertex
      .history()
      .sliding(2)
      .collect {
        case List(event1, event2) if event1.event && event2.event =>
          List(
                  InterlayerEdge(
                          event1.time,
                          event2.time,
                          propertyBuilder(event1.time, event2.time)
                  ),
                  InterlayerEdge(
                          event2.time,
                          event1.time,
                          propertyBuilder(event2.time, event1.time)
                  )
          )
      }
      .flatten
      .toSeq
  }
}
