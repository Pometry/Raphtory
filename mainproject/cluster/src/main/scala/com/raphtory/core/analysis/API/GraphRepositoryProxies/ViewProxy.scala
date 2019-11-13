package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.{ParIterable, ParSet}
import scala.collection.parallel.mutable.ParTrieMap

class ViewProxy(jobID:String, superstep:Int, timestamp:Long, workerID:Int,storage:EntityStorage,managerCount:ManagerCount) extends LiveProxy(jobID,superstep,timestamp,-1,workerID,storage,managerCount) {

  override  def job() = jobID+timestamp
  private var keySet:ParTrieMap[Long,Vertex] = storage.vertices.filter(v=> v._2.aliveAt(timestamp))

  override def getVerticesSet(): ParTrieMap[Long,Vertex] = keySet
  //override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(keySet(id.toInt).viewAt(timestamp),job(),superstep,this,timestamp,-1)
  override def getVertex(v : Vertex)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = {
     new VertexVisitor(v.viewAt(timestamp),job(),superstep,this,timestamp,-1)
  }
  override def latestTime:Long = timestamp

  override def checkVotes(workerID: Int):Boolean = {
    keySet.size == voteCount.get
  }
}