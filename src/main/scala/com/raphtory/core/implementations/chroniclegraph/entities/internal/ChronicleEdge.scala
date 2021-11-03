package com.raphtory.core.implementations.chroniclegraph.entities.internal

import net.openhft.chronicle.map.ChronicleMap

import scala.collection.convert.ImplicitConversions.`map AsScalaConcurrentMap`


class ChronicleEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
  extends ChronicleEntity(msgTime, initialValue) {

  def killList(vKills: List[Long]): Unit = history ++= vKills.map(x=>(x,false))

  def getSrcId: Long   = srcId
  def getDstId: Long   = dstId

  override def createHistory(): ChronicleMap[Long, Boolean] = ???
}