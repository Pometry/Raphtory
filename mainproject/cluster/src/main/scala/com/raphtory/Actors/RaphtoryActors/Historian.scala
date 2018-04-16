package com.raphtory.Actors.RaphtoryActors

import com.raphtory.GraphEntities.{EntitiesStorage, Entity}

//TODO decide how to do shrinking window as graph expands
//TODO implement temporal/spacial profiles
//TODO join historian to cluster

class Historian(entityStorage:EntitiesStorage.type,maximumHistory:Int,compressionWindow:Int,maximumMem:Double) extends RaphtoryActor {

  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val runtime = Runtime.getRuntime

  val maximumHistoryMils = maximumHistory * 60000 //set in minutes
  val compressionWindowMils = compressionWindow * 1000 //set in seconds

  override def receive: Receive = {
    case "archive"=> archive()
    case "compress" => compressGraph()
  }

  def archive() ={
    //TODO make sure a compression has run before archive
    if(!spaceForExtraHistory) { //first check edges
      for (e <- entityStorage.edges){
        checkMaximumHistory(e._2)
      }
    }
    if(!spaceForExtraHistory) { //then check vertices
      for (e <- entityStorage.vertices){
        checkMaximumHistory(e._2)
      }
    }
  }

  def compressGraph() = {
    for (e <- entityStorage.edges){
      compressHistory(e._2)
    }
    for (e <- entityStorage.vertices){
      compressHistory(e._2)
    }
  }

  def checkMaximumHistory(e:Entity) = {
      val (placeholder, allOld, ancientHistory) = e.returnAncientHistory(System.currentTimeMillis - maximumHistoryMils)
      if (placeholder) {
        //TODO decide what to do with placeholders
      }
      if (allOld) {
        entityStorage.oldEntity(e) //TODO finish this method
      }
      //TODO maybe store data in redis?

      for ((k, v) <- e.properties) {
        v.removeAndReturnOldHistory(System.currentTimeMillis - maximumHistoryMils)
        //TODO store properties in Redis? or is this done during compression
      }
  }

  def compressHistory(e:Entity) ={
    val compressedHistory = e.compressAndReturnOldHistory(cutOff)
    if(compressedHistory.nonEmpty){
      //TODO save to redis
      //TODO  decide if compressed history is rejoined
      e.rejoinHistory(compressedHistory)
    }
  }

  def cutOff = System.currentTimeMillis() - compressionWindowMils

  def spaceForExtraHistory = if((runtime.freeMemory/runtime.totalMemory()) < (1-maximumMem)) true else false //check if used memory less than set maximum

}
