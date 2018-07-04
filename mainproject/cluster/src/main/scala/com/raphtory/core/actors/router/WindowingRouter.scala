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



  // Let's call the super.parseJSON in the Router implementation to get Kamon Metrics
  override def parseJSON(command: String) = {
    super.parseJSON(command)
  }

  override def preStart() {
    super.preStart()  //ASK BEN for SCHEDULES
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,CheckVertex)
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS),self,CheckEdges)
  }

  def otherMessages(rcvdMessage : Any) = {
    rcvdMessage match {
      case CheckVertex => checkVertex()
      case CheckEdges => checkEdges()
    }
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

  protected def addVertex(srcId: Int): Unit = {
    vertexWindow.synchronized {
      vertexWindow.get(srcId) match {
        case Some(v) => {
          vertexWindow.update(srcId, System.currentTimeMillis())
          println(s"${System.currentTimeMillis()} vertex update with src: $srcId")
        }
        case None => {
          vertexWindow.put(srcId, System.currentTimeMillis())
          println(s"${System.currentTimeMillis()} vertex added with src: $srcId")
        }

      }
    }
  }

  protected def addEdge(srcId: Int, dstId: Int): Unit = {
    val index: Long = getEdgeIndex(srcId, dstId)
    edgeWindow.synchronized {
      edgeWindow.get(index) match {
        case Some(v) => {
          edgeWindow.update(index, System.currentTimeMillis())
          println(s"${System.currentTimeMillis()} edge updated with src: $srcId, dst: $dstId")
        }

        case None => {
          edgeWindow.put(index, System.currentTimeMillis())
          println(s"${System.currentTimeMillis()} edge added with src: $srcId, dst: $dstId")
        }

      }
    }
    addVertex(srcId)
    addVertex(dstId)
  }

  protected def checkVertex() = {
    println("Checking verteces")
    if (vertexWindow.nonEmpty) {
      vertexWindow foreach { case(k,v) =>
          if (System.currentTimeMillis() - v > WindowSize) {
            mediator ! DistributedPubSubMediator.Send(getManager(k,managerCount),VertexRemoval(System.currentTimeMillis(),k),false)
            vertexWindow.remove(k)
            println(s"${System.currentTimeMillis()} vertex removed with src: $k")
          }

      }
    }
  }

  protected def checkEdges() = {
    println("Checking edges")
    //loop map and see if currentTime - storedTime > WindowSize
    if (edgeWindow.nonEmpty) {
      edgeWindow foreach { case(k,v) =>
        if (System.currentTimeMillis() - v > WindowSize) {
          val srcId: Int = getIndexLO(k) //ASK BEN
          val dstId: Int = getIndexHI(k)
          mediator ! DistributedPubSubMediator.Send(getManager(srcId,managerCount),EdgeRemoval(System.currentTimeMillis(),srcId,dstId),false) //send the srcID, dstID to graph manager
          edgeWindow.remove(k)
          println(s"${System.currentTimeMillis()} edge removed with src: $srcId, dst: $dstId")
        }

      }
    }
  }

}