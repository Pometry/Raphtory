package com.raphtory.Actors.RaphtoryActors

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.GraphEntities._
import com.raphtory.caseclass._
import kamon.Kamon
import kamon.metric.GaugeMetric

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
  var managerCount : Int = managerCountVal
  val managerID    : Int = id                   //ID which refers to the partitions position in the graph manager map

  val printing: Boolean = false                  // should the handled messages be printed to terminal
  val kLogging: Boolean = System.getenv().getOrDefault("PROMETHEUS", "true").trim().toBoolean // should the state of the vertex/edge map be output to Kamon/Prometheus
  val stdoutLog:Boolean = System.getenv().getOrDefault("STDOUT_LOG", "true").trim().toBoolean // A slower logging for the state of vertices/edges maps to Stdout

  val messageCount          : AtomicInteger = new AtomicInteger(0)         // number of messages processed since last report to the benchmarker
  val secondaryMessageCount : AtomicInteger = new AtomicInteger(0)
  val secondaryCounting     = false     // count all messages or just main incoming ones

  val mediator : ActorRef   = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  val storage = EntitiesStorage.apply(printing, managerCount, managerID, mediator)

  mediator ! DistributedPubSubMediator.Put(self)

  val verticesGauge : GaugeMetric = Kamon.gauge("raphtory.vertices")
  val edgesGauge    : GaugeMetric = Kamon.gauge("raphtory.edges")


  implicit val s : Scheduler = Scheduler(ExecutionModel.BatchedExecution(1024))

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def preStart() {
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(1, SECONDS), self, "tick")
    //context.system.scheduler.schedule(Duration(13, SECONDS),
    //    Duration(30, MINUTES), self, "profile")
    context.system.scheduler.schedule(Duration(1, SECONDS),
        Duration(1, MINUTES), self, "stdoutReport")
    context.system.scheduler.schedule(Duration(8, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }


  override def receive : Receive = {
    case "tick"       => reportIntake()
    case "profile"    => profile()
    case "keep_alive" => keepAlive()
    case "stdoutReport"=> Task.eval(reportStdout()).fork.runAsync

    //case LiveAnalysis(name,analyser) => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)

    case VertexAdd(msgTime,srcId)                                => Task.eval(storage.vertexAdd(msgTime,srcId)).fork.runAsync.onComplete(_ => vHandle(srcId))
    case VertexRemoval(msgTime,srcId)                            => Task.eval(storage.vertexRemoval(msgTime,srcId)).fork.runAsync.onComplete(_ => vHandle(srcId))
    case VertexAddWithProperties(msgTime,srcId,properties)       => Task.eval(storage.vertexAdd(msgTime,srcId,properties)).fork.runAsync.onComplete(_ => vHandle(srcId))

    case EdgeAdd(msgTime,srcId,dstId)                            => Task.eval(storage.edgeAdd(msgTime,srcId,dstId)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))
    case RemoteEdgeAdd(msgTime,srcId,dstId,properties)           => Task.eval(storage.remoteEdgeAdd(msgTime,srcId,dstId,properties)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteEdgeAddNew(msgTime,srcId,dstId,properties,deaths) => Task.eval(storage.remoteEdgeAddNew(msgTime,srcId,dstId,properties,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case EdgeAddWithProperties(msgTime,srcId,dstId,properties)   => Task.eval(storage.edgeAdd(msgTime,srcId,dstId,properties)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))

    case EdgeRemoval(msgTime,srcId,dstId)                        => Task.eval(storage.edgeRemoval(msgTime,srcId,dstId)).fork.runAsync.onComplete(_ => eHandle(srcId,dstId))
    case RemoteEdgeRemoval(msgTime,srcId,dstId)                  => Task.eval(storage.remoteEdgeRemoval(msgTime,srcId,dstId)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteEdgeRemovalNew(msgTime,srcId,dstId,deaths)        => Task.eval(storage.remoteEdgeRemovalNew(msgTime,srcId,dstId,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))

    case ReturnEdgeRemoval(msgTime,srcId,dstId)                  => Task.eval(storage.returnEdgeRemoval(msgTime,srcId,dstId)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))
    case RemoteReturnDeaths(msgTime,srcId,dstId,deaths)          => Task.eval(storage.remoteReturnDeaths(msgTime,srcId,dstId,deaths)).fork.runAsync.onComplete(_ => eHandleSecondary(srcId,dstId))

    case UpdatedCounter(newValue) =>
      managerCount = newValue
      println(s"A new PartitionManager has joined the cluster: $newValue")

  }

  def keepAlive() : Unit = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), localAffinity = false)

  /*****************************
   * Metrics reporting methods *
   *****************************/
  def getEntitiesPrevStates[T,U <: Entity](m : TrieMap[T, U]) : Int = {
    var ret : Int = 0
    m.foreach[Unit](e => {
      ret += e._2.getPreviousStateSize()
    })
    ret
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map : TrieMap[T, U]) : Unit = {
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

  def reportStdout() : Unit = {
    if (stdoutLog)
      println(s"TrieMaps size: ${storage.edges.size}\t${storage.vertices.size} | " +
              s"TreeMaps size: ${getEntitiesPrevStates(storage.edges)}\t${getEntitiesPrevStates(storage.vertices)}")
  }
  def reportIntake() : Unit = {
    if(printing)
      println(messageCount)

    // Kamon monitoring
    if (kLogging) {
      kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount.intValue())
      kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString).set(secondaryMessageCount.intValue())
      reportSizes(edgesGauge, storage.edges)
      reportSizes(verticesGauge, storage.vertices)
    }

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
