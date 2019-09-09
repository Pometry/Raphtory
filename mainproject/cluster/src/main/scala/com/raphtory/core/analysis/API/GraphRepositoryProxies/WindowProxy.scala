package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage

import scala.collection.parallel.mutable.ParTrieMap

class WindowProxy(jobID:String, superstep:Int, timestamp:Long, windowSize:Long, workerID:WorkerID) extends LiveProxy(jobID,superstep,timestamp,windowSize,workerID) {

  private var setWindow = windowSize
  private var keySet:ParTrieMap[Int,Vertex] = EntityStorage.vertices(workerID.ID).filter(v=> v._2.aliveAtWithWindow(timestamp,windowSize))

  override  def job() = jobID+timestamp+windowSize

  override def getVerticesSet(): Array[Int] = keySet.keys.toArray

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = {
    keySet.get(id.toInt) match {
      case Some(v) => new VertexVisitor(keySet(id.toInt).viewAtWithWindow(timestamp,setWindow),job(),superstep,this,timestamp,setWindow)
      case None => println(keySet);throw new Exception()
    }
  }

  override def latestTime:Long = timestamp

  def shrinkWindow(newWindowSize:Long) = {
    setWindow = newWindowSize
    keySet = keySet.filter(v=> (v._2).aliveAtWithWindow(timestamp,setWindow))
  }

  override def checkVotes(workerID: Int):Boolean = {
    //println(s"$workerID ${EntityStorage.vertexKeys(workerID).size} $voteCount")
    keySet.size == voteCount
  }
}
