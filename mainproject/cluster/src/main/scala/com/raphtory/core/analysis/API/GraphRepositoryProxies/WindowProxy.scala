package com.raphtory.core.analysis.API.GraphRepositoryProxies

import akka.actor.ActorContext
import com.raphtory.core.analysis.API.VertexVisitor
import com.raphtory.core.analysis.API.{ManagerCount, WorkerID}
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.storage.EntityStorage

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.{ParIterable, ParSet}
import scala.collection.parallel.mutable.ParTrieMap

class WindowProxy(jobID:String, superstep:Int, timestamp:Long, windowSize:Long, workerID:WorkerID) extends LiveProxy(jobID,superstep,timestamp,windowSize,workerID) {

  private var setWindow = windowSize
  private var keySet:ParTrieMap[Int,Vertex] = EntityStorage.vertices(workerID.ID).filter(v=> v._2.aliveAtWithWindow(timestamp,windowSize))
  var timeTest = ArrayBuffer[Long]()

  override def job() = jobID+timestamp+setWindow

  override def getVerticesSet(): Iterator[Int] = keySet.keys.toIterator

  override def getVertex(id : Long)(implicit context : ActorContext, managerCount : ManagerCount) : VertexVisitor = {
    val startTime = System.nanoTime()
    keySet.get(id.toInt) match {
      case Some(v) => {
        val x = new VertexVisitor(keySet(id.toInt).viewAtWithWindow(timestamp,setWindow),job(),superstep,this,timestamp,setWindow)
        //println(System.nanoTime()-startTime)
        timeTest += (System.nanoTime()-startTime)/1000
        x
      }
      case None => println(keySet);throw new Exception()
    }
    //

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
