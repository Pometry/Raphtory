package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.storage.EntityStorage

class ViewProxy(jobID:String, superstep:Int, timestamp:Long, workerID:WorkerID) extends LiveProxy(jobID,superstep,timestamp,-1) {
  private val keySet:Array[Int] = EntityStorage.vertexKeys(workerID.ID).filter(v=> EntityStorage.vertices(v).aliveAt(timestamp)).toArray


  override  def job() = jobID+timestamp

  override def getVerticesSet()(implicit workerID:WorkerID): Array[Int] = keySet

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(EntityStorage.vertices(id.toInt).viewAt(timestamp),job(),superstep,this,timestamp,-1)

  override def latestTime:Long = timestamp

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
