package com.raphtory.core.actors.router

import com.raphtory.core.model.communication._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.utils.Utils.{getEdgeIndex, getManager}
import monix.execution.{ExecutionModel, Scheduler}
import kamon.Kamon
import monix.eval.Task
import com.raphtory.core.utils.Utils._

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration.{Duration, SECONDS}

trait WindowingRouter extends  RouterTrait {
  protected val edgeWindow = ParTrieMap[Long, Long]()
  protected val vertexWindow = ParTrieMap[Int, Long]()
  protected var WindowSize = 5000
  protected var vertexCheckFr = 1
  protected var edgesCheckFr = 1

  var edgeCountTrait: Long = 1
  var edgeTimeTrait: Long = 0
  var vertexCountTrait: Long = 1
  var vertexTimeTrait: Long = 0



  // Let's call the super.parseJSON in the Router implementation to get Kamon Metrics
  override def parseJSON(command: String) = {
    super.parseJSON(command)
  }

  override def preStart() {
    super.preStart()  //ASK BEN for SCHEDULES
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(vertexCheckFr, SECONDS),self,CheckVertex)
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(edgesCheckFr, SECONDS),self,CheckEdges)
    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(1, SECONDS),self,EdgeAvgTrait)
    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(1, SECONDS),self,VertexAvgTrait)
  }

  def otherMessages(rcvdMessage : Any) = {
    rcvdMessage match {
      case CheckVertex => Task.eval(checkVertex()).fork.runAsync
      case CheckEdges => Task.eval(checkEdges()).fork.runAsync

      case EdgeAvgTrait => edgeTimeTraitAvg()
      case VertexAvgTrait => vertexTimeTraitAvg()
      case _ => otherOtherMessages(rcvdMessage)
    }
  }

  def otherOtherMessages(rcvdMessage : Any) = {

  }

  def edgeTimeTraitAvg(): Unit = {
    val avg = edgeTimeTrait/edgeCountTrait
    //println(s"Edge Check Avg is $avg")
    kGauge.refine("actor" -> "Router", "name" -> "edgeTimeTrait").set(avg)
    edgeTimeTrait = 0
    edgeCountTrait = 1
  }

  def vertexTimeTraitAvg(): Unit = {
    val avg = vertexTimeTrait/vertexCountTrait
    //println(s"Vertex Check Avg is $avg")
    kGauge.refine("actor" -> "Router", "name" -> "vertexTimeTrait").set(avg)
    vertexTimeTrait = 0
    vertexCountTrait = 1
  }

  private var count = 0
  private var managerCount: Int = initialManagerCount

  private def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RouterUp(routerId), false)

  private def tick() = {
    kGauge.refine("actor" -> "Router", "name" -> "count").set(count)
    count = 0
  }

  private def newPmJoined(newValue: Int) = if (managerCount < newValue) {
    managerCount = newValue
  }

  protected def addVertex(srcId: Int): Long = {
    val time1 = System.nanoTime()
    vertexWindow.synchronized { //Why not synchronize only on the update ???
      vertexWindow.get(srcId) match {
        case Some(v) => {
          vertexWindow.update(srcId, System.currentTimeMillis())
          //println(s"${System.currentTimeMillis()} vertex update with src: $srcId from $routerId")
        }
        case None => {
          vertexWindow.put(srcId, System.currentTimeMillis())
          //println(s"${System.currentTimeMillis()} vertex added with src: $srcId from $routerId")
        }

      }
    }
    val time2 = System.nanoTime()
    time2 - time1
  }

  protected def addEdge(srcId: Int, dstId: Int): Long = {
    val time1 = System.nanoTime()
    val index: Long = getEdgeIndex(srcId, dstId)
    edgeWindow.synchronized {
      edgeWindow.get(index) match {
        case Some(v) => {
          edgeWindow.update(index, System.currentTimeMillis())
          //println(s"${System.currentTimeMillis()} edge updated with src: $srcId, dst: $dstId from $routerId")
        }
        case None => {
          edgeWindow.put(index, System.currentTimeMillis())
          //println(s"${System.currentTimeMillis()} edge added with src: $srcId, dst: $dstId from $routerId")
        }
      }
    }
    val time2 = System.nanoTime()
    var time = time2 - time1
    time = time + addVertex(srcId)
    time = time + addVertex(dstId)
    time
  }

  protected def checkVertex() = {
    val time1 = System.currentTimeMillis
    //println("Checking verteces")
    if (vertexWindow.nonEmpty) {
      vertexWindow foreach { case(k,v) =>
          if (System.currentTimeMillis() - v > WindowSize) {
            mediator ! DistributedPubSubMediator.Send(getManager(k,managerCount),VertexRemoval(routerId,System.currentTimeMillis(),k),false)
            vertexWindow.remove(k)
            //println(s"${System.currentTimeMillis()} vertex removed with src: $k from $routerId")
          }

      }
    }
    val time2 = System.currentTimeMillis
    val time = time2 - time1
    vertexTimeTrait = vertexTimeTrait + time
    vertexCountTrait += 1
  }

  protected def checkEdges() = {
    val time1 = System.currentTimeMillis
    //println("Checking edges")
    //loop map and see if currentTime - storedTime > WindowSize
    if (edgeWindow.nonEmpty) {
      edgeWindow foreach { case(k,v) =>
        if (System.currentTimeMillis() - v > WindowSize) {
          val srcId: Int = getIndexLO(k) //ASK BEN
          val dstId: Int = getIndexHI(k)
          mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeRemoval(routerId,System.currentTimeMillis(),srcId,dstId),false) //send the srcID, dstID to graph manager
          edgeWindow.remove(k)
          //println(s"${System.currentTimeMillis()} edge removed with src: $srcId, dst: $dstId from $routerId")
        }

      }
    }
    val time2 = System.currentTimeMillis
    var time = time2 - time1
    edgeTimeTrait = edgeTimeTrait + time
    edgeCountTrait += 1
  }

}