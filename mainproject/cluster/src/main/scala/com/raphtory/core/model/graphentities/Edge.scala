package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.{EntityStorage}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
/**
  * Companion Edge object (extended creator for storage loads)
  */
object Edge {
  def apply(workerID:Int, creationTime : Long, srcID:Long, dstID:Long, previousState : mutable.TreeMap[Long, Boolean], properties : ParTrieMap[String, Property], storage:EntityStorage) = {

    val e = new Edge(workerID,creationTime, srcID, dstID, initialValue = true,storage )
    e.previousState   = previousState
    e.properties      = properties
    e
  }

}

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(workerID:Int, msgTime: Long, srcId: Long, dstId: Long, initialValue: Boolean,storage:EntityStorage) extends Entity(msgTime, initialValue,storage) {


  def killList(vKills: mutable.TreeMap[Long, Boolean]): Unit = {
    try{
      removeList ++= vKills
      previousState ++= vKills
    }
    catch {
      case e:java.lang.NullPointerException => {
        e.printStackTrace()
        killList(vKills)
      }
    }
  }

  def getId: Long = dstId //todo remove
  def getSrcId : Long = srcId
  def getDstId : Long= dstId
  def getWorkerID:Int = workerID



//  def getPropertyValuesAfterTime(key : String,time:Long,window:Long) : Option[mutable.TreeMap[Long,Any]] = {
//    if (window == -1L)
//      properties.get(key) match {
//        case Some(p) => Some(p.previousState.filter(x => x._1 <= time))
//        case None => None
//      }
//    else
//      properties.get(key) match {
//        case Some(p) => Some(p.previousState.filter(x => x._1 <= time && time - x._1 <= window))
//        case None => None
//      }
//  }


  def viewAt(time:Long):Edge = {

//    if(time > EntityStorage.lastCompressedAt){
//      throw StillWithinLiveGraphException(time)
//    }
    //if(time < EntityStorage.oldestTime){
    //  throw PushedOutOfGraphException(time)
    //}
    var closestTime:Long = 0
    var value = false
    for((k,v) <- previousState){
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    }
    //if(value==false)
    //  throw EntityRemovedAtTimeException(getId)
    val edge = new Edge(-1,closestTime,srcId,dstId,value,storage)
    for((k,p) <- properties) {
      val value = p.valueAt(time)
      if (!(value equals("")))
        edge + (time,false,k,value)
    }
    edge
  }



  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Edge]){
      val v2 = obj.asInstanceOf[Edge] //add associated edges
      if((getSrcId == v2.getSrcId) && (getDstId == v2.getDstId) && (previousState.equals(v2.previousState)) && (oldestPoint.get == v2.oldestPoint.get) && (newestPoint.get == newestPoint.get) && (properties.equals(v2.properties))){
        return true
      }
    }
    false
  }


  override def toString: String = {
    s"Edge srcID $srcId dstID $dstId \n History $previousState \n Properties:\n $properties"
  }

}
