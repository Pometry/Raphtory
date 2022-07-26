package com.raphtory.api.analysis.visitor

/** Class for representing interlayer edges
  *
  * @param srcTime source time-stamp for the edge
  * @param dstTime destination time-stamp for the edge
  * @param properties properties that the interlayer edge should have
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

  override def firstActivityAfter(time: Long, strict: Boolean): Option[HistoricEvent] = None

  override def lastActivityBefore(time: Long, strict: Boolean): Option[HistoricEvent] = None

  override def getPropertySet(): List[String] = properties.keys.toList

  override def getPropertyHistory[T](
      key: String,
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Option[List[PropertyValue[T]]] =
    properties.get(key) match {
      case None        => None
      case Some(value) =>
        val actualTime = Seq(srcTime, dstTime, after).min
        if (actualTime > before)
          Some(List.empty)
        else
          Some(List(PropertyValue(actualTime, 0, value.asInstanceOf[T])))
    }

  override def latestActivity(): HistoricEvent =
    HistoricEvent(time = math.max(srcTime, dstTime), index = 0L, event = false)

  override def earliestActivity(): HistoricEvent =
    HistoricEvent(math.min(srcTime, dstTime), 0L, event = true)

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

/** Default builders for constructing interlayer edges
  *
  * @define properties @param properties Map of property values for the interlayer edges
  * @define propertyBuilder @param propertyBuilder Builder function for interlayer edge properties.
  *                         The input parameters for the builder are the timestamps for the two endpoints of the edge.
  */
object InterlayerEdgeBuilders {

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the next activity of the vertex unless it is
    * a deletion event.
    *
    * $properties
    */
  def linkNext(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkNext((_, _) => properties)

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the next activity of the vertex unless it is a
    * deletion event.
    *
    * $propertyBuilder
    */
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

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the prior activity of the vertex unless it is
    * a deletion event.
    *
    * $properties
    */
  def linkPrevious(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkPrevious((_, _) => properties)

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the prior activity of the vertex unless it is
    * a deletion event.
    *
    * $propertyBuilder
    */
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

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the next and prior activity of the vertex unless it is
    * a deletion event.
    *
    * $properties
    */
  def linkPreviousAndNext(properties: Map[String, Any]): Vertex => Seq[InterlayerEdge] =
    linkPreviousAndNext((_, _) => properties)

  /**
    * Add an interlayer edge to the [[ExplodedVertex]] representing the next and prior activity of the vertex unless it is
    * a deletion event.
    *
    * $propertyBuilder
    */
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
