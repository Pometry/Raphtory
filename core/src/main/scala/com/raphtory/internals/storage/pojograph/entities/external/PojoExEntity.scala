package com.raphtory.internals.storage.pojograph.entities.external

import com.raphtory.api.analysis.visitor.EntityVisitor
import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.HistoryView
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.raphtory.internals.storage.pojograph.entities.PojoEntityView
import com.raphtory.internals.storage.pojograph.entities.internal.PojoEntity
import com.raphtory.internals.storage.pojograph.entities.internal.Property
import com.raphtory.internals.storage.pojograph.entities.internal.PropertyView
import com.raphtory.utils.OrderingFunctions._

import scala.collection.IndexedSeqView
import scala.collection.mutable
import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint

abstract private[pojograph] class PojoExEntity(
    entity: PojoEntity,
    val view: PojoGraphLens,
    val start: IndexedValue,
    val end: IndexedValue
) extends EntityVisitor
        with PojoEntityView {

  def this(entity: PojoEntity, view: PojoGraphLens) = {
    this(entity, view, TimePoint.first(view.start), TimePoint.last(view.end))
  }

  override lazy val historyView: HistoryView[HistoricEvent] = entity.historyView.slice(start, end.next)
  val properties: mutable.Map[String, PropertyView]         = mutable.Map.empty

  def get(property: String): Option[PropertyView] =
    properties.get(property) match {
      case f: Some[PropertyView] => f
      case None                  =>
        entity.get(property) match {
          case Some(v) =>
            val p = v.viewBetween(start, end)
            properties(property) = p
            Some(p)
          case None    => None
        }
    }

  def Type: String = entity.Type

  def firstActivityAfter(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict) historyView.after(TimePoint.first(time + 1)).headOption
    else historyView.after(TimePoint.first(time)).headOption

  def lastActivityBefore(time: Long, strict: Boolean = true): Option[HistoricEvent] =
    if (strict) historyView.before(TimePoint.last(time - 1)).headOption
    else historyView.before(TimePoint.last(time)).headOption

  def latestActivity(): HistoricEvent = historyView.last

  def earliestActivity(): HistoricEvent = historyView.head

  def getPropertySet(): List[String] =
    entity.properties.filter(p => p._2.earliest <= end).keys.toList

  //  //TODo ADD Before
//  def getPropertyValues[T](key: String, after: Long, before: Long): Option[List[T]] =
//    entity.properties.get(key) match {
//      case Some(p) => Some(p.valuesAfter(after).toList.map(_.asInstanceOf[T]))
//      case None    => None
//    }

  def getPropertyHistory[T](
      key: String,
      after: Long,
      before: Long
  ): Option[List[PropertyValue[T]]] =
    get(key).map(
            _.valueHistory(TimePoint.first(after), TimePoint.last(before)).toList
              .map(_.asInstanceOf[PropertyValue[T]])
    )

  def history(): List[HistoricEvent] =
    historyView.toList
}
