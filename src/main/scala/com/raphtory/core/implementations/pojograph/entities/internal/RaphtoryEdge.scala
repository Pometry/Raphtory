package com.raphtory.core.implementations.pojograph.entities.internal

import com.raphtory.core.model.graph.GraphPartition

import scala.collection.mutable

/**
  * Companion Edge object (extended creator for storage loads)
  */
object RaphtoryEdge {
  def apply(workerID: Int, creationTime: Long, srcID: Long, dstID: Long, previousState: mutable.TreeMap[Long, Boolean], properties: mutable.Map[String, Property], storage: GraphPartition) = {

    val e = new RaphtoryEdge(creationTime, srcID, dstID, initialValue = true)
    e.history = previousState
    e.properties = properties
    e
  }

//  def apply(parquet: ParquetEdge): RaphtoryEdge = {
//    val edge = if(parquet.split)
//      new SplitRaphtoryEdge(parquet.history.head._1, parquet.src, parquet.dst, parquet.history.head._2)
//    else
//      new RaphtoryEdge(parquet.history.head._1, parquet.src, parquet.dst, parquet.history.head._2)
//    parquet.history.foreach(update=> if(update._2) edge.revive(update._1) else edge.kill(update._1))
//    parquet.properties.foreach(prop=> edge.properties +=((prop.key,Property(prop))))
//    edge
//  }

}

/**
  * Created by Mirate on 01/03/2017.
  */
class RaphtoryEdge(msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean)
        extends RaphtoryEntity(msgTime, initialValue) {

  def killList(vKills: List[Long]): Unit = history ++= vKills.map(x=>(x,false))

  def getSrcId: Long   = srcId
  def getDstId: Long   = dstId
//  def serialise(): ParquetEdge = ParquetEdge(srcId,dstId,false,history.toList,properties.map(x=> x._2.serialise(x._1)).toList)
}