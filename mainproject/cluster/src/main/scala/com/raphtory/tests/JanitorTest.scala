package com.raphtory.tests

import com.raphtory.core.model.graphentities.{Entity, Vertex}

object JanitorTest extends App{

  val temporalMode = true //flag denoting if storage should focus on keeping more entities in memory or more history
  val timeWindow = 1 //timeWindow set in seconds
  val timeWindowMils = timeWindow * 1000

  //  val vertex = new Vertex(System.currentTimeMillis(),1,true,false)
  //  Thread.sleep(100)
  //  vertex revive(System.currentTimeMillis())
  //  Thread.sleep(100)
  //  vertex revive(System.currentTimeMillis())
  //  Thread.sleep(100)
  //  vertex revive(System.currentTimeMillis())
  //  Thread.sleep(500)
  //  vertex kill(System.currentTimeMillis())
  //  Thread.sleep(500)
  //  vertex revive(System.currentTimeMillis())

  val vertex = new Vertex(1,1,true,false)
  vertex revive(2)
  vertex revive(3)
  vertex revive(4)
  vertex kill(5)
  vertex revive(6)
  vertex +(1,"prop","val1")
  vertex +(2,"prop","val2")
  vertex +(3,"prop","val2")
  vertex +(4,"prop","val3")
  vertex +(5,"prop","val3")
  vertex +(6,"prop","val3")
  vertex +(7,"prop","val3")

//  println(vertex.previousState)
  println(vertex.properties.getOrElse("prop",null).previousState)
  compressHistory(vertex)
//  println(vertex.previousState)
  println(vertex.properties.getOrElse("prop",null).previousState)
  println(vertex.compressionRate())

  def compressHistory(e:Entity) ={
    val compressedHistory = e.compressAndReturnOldHistory(cutOff)
    for ((id,property) <- e.properties){
      val oldHistory = property.compressAndReturnOldHistory(cutOff)
      // savePropertiesToRedis(e, past)
      //store offline
    }
  }

  def cutOff = 8
  //vertex + (1, "Name", "Ben")
  //vertex + (1, "Hair", "Brown")

  // vertex + (3, "Eyes", "Brown")
  // vertex + (1, "Name", "Alessandro")

}