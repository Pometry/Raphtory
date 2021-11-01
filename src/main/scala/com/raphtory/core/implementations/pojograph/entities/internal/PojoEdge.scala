package com.raphtory.core.implementations.pojograph.entities.internal

import com.raphtory.core.implementations.generic.entity.internal.InternalEdge

class PojoEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
  extends PojoEntity(msgTime, initialValue) with InternalEdge {

  def killList(vKills: List[Long]): Unit = history ++= vKills.map(x=>(x,false))

  def getSrcId: Long   = srcId
  def getDstId: Long   = dstId
}