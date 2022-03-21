package com.raphtory.core.storage.pojograph.entities.internal

/** @DoNotDocument */
class PojoEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  def killList(vKills: List[Long]): Unit = {
    historyTime.zipWithIndex.foreach{ case (k, c) =>
      if (vKills.contains(k))
        historyValue.update(c, false)
    }
  }
  // history()._1.zipWithIndex.filter({case (k,c)  => k >= time}).minBy(x => x._2)

  def getSrcId: Long = srcId
  def getDstId: Long = dstId
}
