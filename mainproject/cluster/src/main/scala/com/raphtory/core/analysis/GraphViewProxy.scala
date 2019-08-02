package com.raphtory.core.analysis

import akka.actor.ActorContext
import com.raphtory.core.storage.EntityStorage

class GraphViewProxy(jobID:String,superstep:Int,timestamp:Long,workerID:WorkerID) extends GraphRepoProxy(jobID,superstep) {
  private val keySet:Array[Int] = EntityStorage.vertexKeys(workerID.ID).filter(v=> EntityStorage.vertices(v).aliveAt(timestamp)).toArray


  override  def job() = jobID+timestamp

  override def getVerticesSet()(implicit workerID:WorkerID): Array[Int] = keySet

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(EntityStorage.vertices(id.toInt).viewAt(timestamp),job(),superstep,this)

  override def latestTime:Long = timestamp

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
