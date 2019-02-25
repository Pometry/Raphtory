package com.raphtory.core.actors.router.WindowingRouters

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.router.TraditionalRouter.RouterTrait
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils.{getEdgeIndex, getIndexHI, getIndexLO}
import kamon.Kamon
import kamon.metric.GaugeMetric
import monix.eval.Task
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.{Queue, Set}
import scala.concurrent.duration.{Duration, SECONDS}

trait QueueWindowingRouter extends  RouterTrait {
  protected val edgeWindow = new Queue[(Long, Long)]
  protected val vertexWindow = new Queue[(Int, Long)]

  protected var WindowSize = System.getenv().getOrDefault("WINDOW_SIZE", "5").toInt * 1000
  println(s"Window size set to $WindowSize milliseconds")
  protected var QueueCheckFr = 15

  var edgeCountTrait: Long = 1
  var edgeTimeTrait: Long = 0
  var vertexCountTrait: Long = 1
  var vertexTimeTrait: Long = 0
  private val verticesGauge : GaugeMetric = Kamon.gauge("raphtory.vertexCheckingTime")
  private val edgesGauge    : GaugeMetric = Kamon.gauge("raphtory.edgeCheckingTime")

  // Let's call the super.parseJSON in the Router implementation to get Kamon Metrics
  override def parseRecord(command: String) = {
    super.parseRecord(command)
  }

  override def preStart() {
    super.preStart() //ASK BEN for SCHEDULES
    context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS),self,CheckVertex)
    context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS),self,CheckEdges)

    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(1, SECONDS),self,EdgeAvgTrait)
    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(1, SECONDS),self,VertexAvgTrait)
    //context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS), self, "checkVertex")
    //context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS), self, "checkEdges")
  }

  def otherMessages(rcvdMessage: Any) = {
    rcvdMessage match {
      case CheckVertex => peekingVertexQueue()
      case CheckEdges => peekingEdgeQueue()

      case EdgeAvgTrait => edgeTimeTraitAvg()
      case VertexAvgTrait => vertexTimeTraitAvg()
      case _ => otherOtherMessages(rcvdMessage)
      //case CheckVertex => peekingQueue(vertexWindow,checkVertex, CheckEdges)
      //case CheckEdges => peekingQueue(edgeWindow,checkEdges, CheckEdges)
    }
  }

  def otherOtherMessages(rcvdMessage : Any) = {

  }

  def edgeTimeTraitAvg(): Unit = {
    val avg = edgeTimeTrait/edgeCountTrait
    //println(s"Edge Check Avg is $avg")
    edgesGauge.refine("actor" -> "Router", "replica" -> routerId.toString, "name" -> "Checking Time Edges").set(avg)
    edgeTimeTrait = 0
    edgeCountTrait = 1
  }

  def vertexTimeTraitAvg(): Unit = {
    val avg = vertexTimeTrait/vertexCountTrait
    //println(s"Vertex Check Avg is $avg")
    verticesGauge.refine("actor" -> "Router", "replica" -> routerId.toString, "name" -> "Checking Time Vertices").set(avg)
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
    //Just push on the queue
    vertexWindow += ((srcId, System.currentTimeMillis()))
    //println(s"${System.currentTimeMillis()} vertex update with src: $srcId from $routerId")
    val time2 = System.nanoTime()
    time2 - time1
  }

  protected def addEdge(srcId: Int, dstId: Int): Long = {
    val time1 = System.nanoTime()
    //Just push on the queue
    val index: Long = getEdgeIndex(srcId, dstId)

    edgeWindow += ((index, System.currentTimeMillis()))
    //println(s"${System.currentTimeMillis()} edge added with src: $srcId, dst: $dstId from $routerId")

    val time2 = System.nanoTime()
    var time = time2 - time1
    time = time + addVertex(srcId)
    time = time + addVertex(dstId)
    time
  }

  protected def checkVertex(id: Int) = {
    val time1 = System.nanoTime()
    //println("Checking verteces")
    //Called by peekingQueue function to check if any other vertex in the queue then delete if not
    if (vertexWindow.nonEmpty) {
      if(!vertexWindow.exists(_._1 == id)){
        toPartitionManager(VertexRemoval(routerId,System.currentTimeMillis(),id))
        //mediator ! DistributedPubSubMediator.Send(getManager(id, managerCount), VertexRemoval(routerId, System.currentTimeMillis(), id), false)
        //println(s"${System.currentTimeMillis()} vertex removed with src: $id from $routerId")
      }
    }
    val time2 = System.nanoTime()
    val time = time2 - time1
    vertexTimeTrait = vertexTimeTrait + time
    vertexCountTrait += 1
  }

  protected def checkEdges(index: Long) = {
    val time1 = System.nanoTime()
    //println("Checking edges")
    //Called by peekingQueue function to check if any other Index in the queue then delete if not
    if (edgeWindow.nonEmpty) {
      if(!edgeWindow.exists(_._1 == index)) {
        val dstId: Int = getIndexHI(index)
        val srcId: Int = getIndexLO(index)
        toPartitionManager(EdgeRemoval(routerId,System.currentTimeMillis(),srcId,dstId))
        //mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount), EdgeRemoval(routerId, System.currentTimeMillis(), srcId, dstId), false)
        //println(s"${System.currentTimeMillis()} edge removed with src: $srcId, dst: $dstId from $routerId")
      }
    }
    val time2 = System.nanoTime()
    var time = time2 - time1
    edgeTimeTrait = edgeTimeTrait + time
    edgeCountTrait += 1
  }

  protected def peekingQueue[T <: AnyVal](queueToBeChecked: Queue[(T,Long)], checkFunction: (T) => Unit, msgType: CheckVertex ) = {
    //Made Generalized for both queues with Queues and functions given as attribute
    //Peeking at the queue and keep removing until in the windowing period and add ids in set
    //println(s"Peeking at the queue ${queueToBeChecked}")
    val uniqueIDs = Set[T]()
    var moreToPop = true
    while(moreToPop){
      if(!queueToBeChecked.isEmpty){
        if(System.currentTimeMillis() - queueToBeChecked.head._2 > WindowSize){
          uniqueIDs += queueToBeChecked.dequeue._1
        }
        else
          moreToPop = false
      }
      moreToPop = false
    }
    //For each Id in set check if other instance in the queue by calling check function
    uniqueIDs foreach(t => checkFunction(t)) //to become multi threaded

    //Call the function again to keep peeking at the queue
    context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS), self,msgType)
  }

  protected def peekingVertexQueue() = {
    //Made Generalized for both queues with Queues and functions given as attribute
    //Peeking at the queue and keep removing until in the windowing period and add ids in set
    //println(s"Peeking at the Vertex queue ")
    val uniqueIDs = Set[Int]()
    var moreToPop = true
    while(moreToPop){
      if(!vertexWindow.isEmpty){
        if(System.currentTimeMillis() - vertexWindow.head._2 > WindowSize){
          uniqueIDs += vertexWindow.dequeue._1
        }
        else
          moreToPop = false
      }
      moreToPop = false
    }
    //For each Id in set check if other instance in the queue by calling check function

    //uniqueIDs foreach(t => checkVertex(t)) //SINGLE THREADED
    uniqueIDs foreach(t => checkVertex(t)) //MULTI THREADED

    //Call the function again to keep peeking at the queue
    //context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS), self,msgType)
    context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS),self,CheckVertex)
  }
  protected def peekingEdgeQueue() = {
    //Made Generalized for both queues with Queues and functions given as attribute
    //Peeking at the queue and keep removing until in the windowing period and add ids in set
    //println(s"Peeking at the Edges queue ")
    val uniqueIDs = Set[Long]()
    var moreToPop = true
    while(moreToPop){
      if(!edgeWindow.isEmpty){
        if(System.currentTimeMillis() - edgeWindow.head._2 > WindowSize){
          uniqueIDs += edgeWindow.dequeue._1
        }
        else
          moreToPop = false
      }
      moreToPop = false
    }
    //For each Id in set check if other instance in the queue by calling check function

    //uniqueIDs foreach(t => checkEdges(t)) //SINGLE THREADED
    uniqueIDs foreach(t => checkEdges(t)) //MULTI THREADED

    //Call the function again to keep peeking at the queue
    //context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS), self,msgType)
    context.system.scheduler.scheduleOnce(Duration(QueueCheckFr, SECONDS),self,CheckEdges)
  }
}
