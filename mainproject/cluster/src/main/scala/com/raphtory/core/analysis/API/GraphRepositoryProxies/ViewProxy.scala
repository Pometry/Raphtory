package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.{ParIterable, ParSet}
import scala.collection.parallel.mutable.ParTrieMap

class ViewProxy(jobID:String, superstep:Int, timestamp:Long, workerID:WorkerID) extends LiveProxy(jobID,superstep,timestamp,-1,workerID) {

  private val keySet:ParIterable[Int] = EntityStorage.vertices(workerID.ID).filter(v=> v._2.aliveAt(timestamp)).keySet
  override  def job() = jobID+timestamp

  override def getVerticesSet(): Iterator[Int] = keySet.toIterator

  //override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = new VertexVisitor(keySet(id.toInt).viewAt(timestamp),job(),superstep,this,timestamp,-1)
  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = {
    println(id)
    EntityStorage.vertices(workerID.ID).get(id.toInt) match {
      case Some(v) => new VertexVisitor(v.viewAt(timestamp),job(),superstep,this,timestamp,-1)
      case None => throw new Exception()
    }
  }
  override def latestTime:Long = timestamp

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
