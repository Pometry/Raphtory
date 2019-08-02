package com.raphtory.core.analysis.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.{ManagerCount, VertexVisitor, WorkerID}
import com.raphtory.core.storage.EntityStorage

class WindowProxy(jobID:String, superstep:Int, timestamp:Long, windowSize:Long, workerID:WorkerID) extends GraphProxy(jobID,superstep,timestamp,windowSize) {
  private val keySet:Array[Int] = EntityStorage.vertexKeys(workerID.ID).filter(v=> EntityStorage.vertices(v).aliveAtWithWindow(timestamp,windowSize)).toArray

  override  def job() = jobID+timestamp+windowSize

  override def getVerticesSet()(implicit workerID:WorkerID): Array[Int] = keySet

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(EntityStorage.vertices(id.toInt).viewAtWithWindow(timestamp,windowSize),job(),superstep,this,timestamp,windowSize)

  override def latestTime:Long = timestamp

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
