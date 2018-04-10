package com.raphtory.Actors.RaphtoryActors

import java.util.concurrent.atomic.AtomicInteger

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.GraphEntities._
import com.raphtory.caseclass._
import kamon.Kamon

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import monix.eval.Task
import monix.execution.{ExecutionModel, Scheduler}

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionManager(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  var managerCount = managerCountVal
  val managerID  = id                   //ID which refers to the partitions position in the graph manager map

  val printing = false                  // should the handled messages be printed to terminal
  val logging  = false                  // should the state of the vertex/edge map be output to file

  var messageCount          : AtomicInteger = new AtomicInteger(0)         // number of messages processed since last report to the benchmarker
  var secondaryMessageCount : AtomicInteger = new AtomicInteger(0)
  val secondaryCounting     = false     // count all messages or just main incoming ones

  val mediator              = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  val storage = EntitiesStorage.apply(printing, managerCount, managerID, mediator)

  mediator ! DistributedPubSubMediator.Put(self)

  val verticesGauge         = Kamon.gauge("raphtory.vertices")
  val edgesGauge            = Kamon.gauge("raphtory.edges")

  implicit val s = Scheduler(ExecutionModel.BatchedExecution(1024))

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS), self, "tick")
    /*context.system.scheduler.schedule(Duration(13, SECONDS),
        Duration(30, MINUTES), self, "profile")*/
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }


  override def receive : Receive = {
    case "tick"       => reportIntake()
    case "profile"    => profile()
    case "keep_alive" => keepAlive()

    //case LiveAnalysis(name,analyser) => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)

    case VertexAdd(msgId,srcId)                                => Task.eval(storage.vertexAdd(msgId,srcId)).fork.runAsync.onComplete(_ => vHandle(srcId))
    case VertexRemoval(msgId,srcId)                            => Task.eval(storage.vertexRemoval(msgId,srcId)).fork.runAsync.onComplete(_ => vHandle(srcId))
    case VertexAddWithProperties(msgId,srcId,properties)       => Task.eval(storage.vertexAdd(msgId,srcId,properties)).fork.runAsync.onComplete(_ => vHandle(srcId))

    case EdgeAdd(msgId,srcId,dstId)                            => Task.eval(storage.edgeAdd(msgId,srcId,dstId)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))
    case RemoteEdgeAdd(msgId,srcId,dstId,properties)           => Task.eval(storage.remoteEdgeAdd(msgId,srcId,dstId,properties)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteEdgeAddNew(msgId,srcId,dstId,properties,deaths) => Task.eval(storage.remoteEdgeAddNew(msgId,srcId,dstId,properties,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case EdgeAddWithProperties(msgId,srcId,dstId,properties)   => Task.eval(storage.edgeAdd(msgId,srcId,dstId,properties)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))

    case EdgeRemoval(msgId,srcId,dstId)                        => Task.eval(storage.edgeRemoval(msgId,srcId,dstId)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))
    case RemoteEdgeRemoval(msgId,srcId,dstId)                  => Task.eval(storage.remoteEdgeRemoval(msgId,srcId,dstId)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths)        => Task.eval(storage.remoteEdgeRemovalNew(msgId,srcId,dstId,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))

    case ReturnEdgeRemoval(msgId,srcId,dstId)                  => Task.eval(storage.returnEdgeRemoval(msgId,srcId,dstId)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteReturnDeaths(msgId,srcId,dstId,deaths)          => Task.eval(storage.remoteReturnDeaths(msgId,srcId,dstId,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))

    case UpdatedCounter(newValue) => {
      managerCount = newValue
      println(s"A new PartitionManager has joined the cluster: ${newValue}")
    }
  }

  def keepAlive() = {
    mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), false)
  }

  /*****************************
   * Metrics reporting methods *
   *****************************/
  def getEntitiesPrevStates[T,U <: Entity](m : TrieMap[T, U]) : Int = {
    var ret : Int = 0
    m.foreach[Unit](e => {
      ret += e._2.previousState.size
    })
    ret
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map : TrieMap[T, U]) = {
    def getGauge(name : String) = {
     g.refine("actor" -> "PartitionManager", "replica" -> id.toString, "name" -> name)
    }

    Task.eval(getGauge("Total number of entities").set(map.size)).fork.runAsync
    //getGauge("Total number of properties") TODO

    Task.eval(getGauge("Total number of previous states").set(
      getEntitiesPrevStates(map)
    )).fork.runAsync

    // getGauge("Number of props previous history") TODO
  }

  def reportIntake() : Unit = {
    if(printing)
      println(messageCount)

    // Kamon monitoring
    kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount").set(messageCount.intValue())
    kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount").set(secondaryMessageCount.intValue())

    reportSizes(edgesGauge, storage.edges)
    reportSizes(verticesGauge, storage.vertices)

    // Heap benchmarking
    //profile()

    // Reset counters
    messageCount.set(0)
    secondaryMessageCount.set(0)
  }

  def vHandle(srcID : Int) : Unit = {
    messageCount.incrementAndGet()
  }

  def vHandleSecondary(srcID : Int) : Unit = {
    secondaryMessageCount.incrementAndGet()
  }
  def eHandle(srcID : Int, dstID : Int) : Unit = {
    messageCount.incrementAndGet()
  }

  def eHandleSecondary(srcID : Int, dstID : Int) : Unit = {
    secondaryMessageCount.incrementAndGet()
  }

  /*********************************
   * END Metrics reporting methods *
   *********************************/
}
