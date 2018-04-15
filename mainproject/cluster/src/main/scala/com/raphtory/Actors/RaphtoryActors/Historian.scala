package com.raphtory.Actors.RaphtoryActors

import com.raphtory.GraphEntities.Entity

class Historian extends RaphtoryActor {

  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val timeWindow = 1 //timeWindow set in seconds
  val timeWindowMils = timeWindow * 1000

  override def receive: Receive = {
    case "temp"=> cutOff
  }

  def compressHistory(e:Entity) ={
    val compressedHistory = e.removeAndReturnOldHistory(cutOff)
    //TODO save to redis
    //TODO prehaps decide if compressed history is rejoined
    e.rejoinCompressedHistory(compressedHistory)
  }

  def cutOff = System.currentTimeMillis() - timeWindowMils

}
