package com.raphtory.tests

import com.raphtory.core.model.graphentities.Vertex
import com.redis._

import scala.collection.concurrent.TrieMap

object RedisTest extends App {
  val r = new RedisClient("localhost", 6379)

  val vertex = new Vertex(1,1,true,false)
  vertex kill(2)
  vertex revive(3)

  vertex + (1, "Name", "Ben")
  vertex + (1, "Hair", "Brown")

  vertex + (3, "Eyes", "Brown")
  vertex + (1, "Name", "Alessandro")

  storeInRedis(vertex)
  getVertexHistory(vertex.vertexId)
  def storeInRedis(v:Vertex)={
    storeVertexHistory(v)
  }

  def storeVertexHistory(v:Vertex)={
    for(point <- v.previousState){
      val hashKey = s"${v.vertexId}-t${point._1}"
      r.hmset(hashKey,TrieMap[String,Boolean](("present",point._2)))
      //create the hash and save in redis

      val setKey = s"${v.vertexId}"
      r.zadd(setKey,point._1,hashKey)
      //add the hash to the sorted set for this entity
    }
  }

  def getVertexHistory(vId:Int)={
    r.zrange(vId, 0, 10) match {
      case Some(list) => {
        for(hashID <- list){
          r.hgetall1(hashID) match {
            case Some(map) =>{
              println(map("present"))
            }
            case None => println("No hash")
          }
        }
      }
      case None => println("No Data Lad")
    }

  }

}
