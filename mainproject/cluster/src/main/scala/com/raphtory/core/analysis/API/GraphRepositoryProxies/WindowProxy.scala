package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ParIterable, ParSet}
import scala.collection.parallel.ParMap

class WindowProxy(jobID:String, superstep:Int, timestamp:Long, windowSize:Long, workerID:WorkerID,context : ActorContext, managerCount : ManagerCount) extends LiveProxy(jobID,superstep,timestamp,windowSize,workerID,context,managerCount) {

  private var setWindow = windowSize
  var keySet:ParMap[Int,VertexVisitor] = EntityStorage.vertices(workerID.ID).filter(v=> v._2.aliveAtWithWindow(timestamp,windowSize))
                                                                                    .mapValues(x=>new VertexVisitor(x.viewAtWithWindow(timestamp,setWindow),job(),superstep,this,timestamp,setWindow))
  override def job() = jobID+timestamp+setWindow

  override def getVerticesSet(): ParSet[Int] = keySet.keySet

  override def getVertex(id : Long) : VertexVisitor = {
    keySet.get(id.toInt) match {
      case Some(v) => v
      case None => println("howdy");throw new Exception()
    }
  }

  override def latestTime:Long = timestamp

  def shrinkWindow(newWindowSize:Long) = {
    setWindow = newWindowSize
    val x = keySet.size
    keySet = keySet.filter(v=> (v._2).aliveAtWithWindow(timestamp,setWindow))
    //println(s"$workerID $timestamp $newWindowSize keyset prior $x keyset after ${keySet.size}")
  }

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
