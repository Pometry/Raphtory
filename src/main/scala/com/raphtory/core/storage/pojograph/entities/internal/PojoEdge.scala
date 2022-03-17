package com.raphtory.core.storage.pojograph.entities.internal

/** @DoNotDocument */
class PojoEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends PojoEntity(msgTime, initialValue) {

  def killList(vKills: List[Long]): Unit = history.long2BooleanEntrySet().forEach(entity => if (vKills.contains(entity.getLongKey)) entity.setValue(false))

  def getSrcId: Long = srcId
  def getDstId: Long = dstId
}
