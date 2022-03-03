package com.raphtory.core.storage.pojograph.entities.internal

/** @DoNotDocument */
class PojoEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  def killList(vKills: List[Long]): Unit = history ++= vKills.map(x => (x, false))

  def getSrcId: Long = srcId
  def getDstId: Long = dstId
}
