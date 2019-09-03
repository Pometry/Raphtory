package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.storage.EntityStorage

class WindowProxy(jobID:String, superstep:Int, timestamp:Long, windowSize:Long, workerID:WorkerID) extends LiveProxy(jobID,superstep,timestamp,windowSize) {

  private var setWindow = windowSize

  private var keySet:Array[Int] = EntityStorage.vertexKeys(workerID.ID).filter(v=> EntityStorage.vertices(v).aliveAtWithWindow(timestamp,windowSize)).toArray

  override  def job() = jobID+timestamp+windowSize

  override def getVerticesSet()(implicit workerID:WorkerID): Array[Int] = keySet

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(EntityStorage.vertices(id.toInt).viewAtWithWindow(timestamp,setWindow),job(),superstep,this,timestamp,setWindow)

  override def latestTime:Long = timestamp

  def shrinkWindow(newWindowSize:Long) = {
    setWindow = newWindowSize
    keySet = keySet.filter(v=> EntityStorage.vertices(v).aliveAtWithWindow(timestamp,setWindow))
  }

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
