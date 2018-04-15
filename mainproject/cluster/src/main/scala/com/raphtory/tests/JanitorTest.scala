package com.raphtory.tests

import com.raphtory.GraphEntities.{Entity, Vertex}

import scala.collection.mutable

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
  println(vertex.previousState)
  compressHistory(vertex)
  println(vertex.previousState)

  def compressHistory(e:Entity) ={
    val compressedHistory = e.removeAndReturnOldHistory(cutOff)
    //TODO save to redis
    //TODO prehaps decide if compressed history is rejoined
    e.rejoinCompressedHistory(compressedHistory)
  }

  def cutOff = System.currentTimeMillis() - timeWindowMils
  //vertex + (1, "Name", "Ben")
  //vertex + (1, "Hair", "Brown")

 // vertex + (3, "Eyes", "Brown")
 // vertex + (1, "Name", "Alessandro")

}
