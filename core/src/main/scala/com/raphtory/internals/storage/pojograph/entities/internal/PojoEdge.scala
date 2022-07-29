package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.storage.pojograph.OrderedBuffer._

private[raphtory] class PojoEdge(msgTime: Long, index: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, index, initialValue) {

  def killList(vKills: List[(Long, Long)]): Unit =
    historyView.extend(vKills.map(x => HistoricEvent(x._1, x._2, false)))

  def getSrcId: Long = srcId
  def getDstId: Long = dstId
}
