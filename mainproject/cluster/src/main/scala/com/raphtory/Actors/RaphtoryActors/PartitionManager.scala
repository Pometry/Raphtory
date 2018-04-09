package com.raphtory.Actors.RaphtoryActors

import java.io._

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.GraphEntities._
import com.raphtory.caseclass._
import kamon.Kamon

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import com.raphtory.utils.Utils._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import scala.collection.mutable
/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionManager(id : Int, test : Boolean, managerCountVal : Int) extends RaphtoryActor {
  var managerCount = managerCountVal
  val managerID  = id                         //ID which refers to the partitions position in the graph manager map

  /**
    * Graph model maps
    */
  var vertices = TrieMap[Int,Vertex]()      // Map of Vertices contained in the partition
  var edges    = TrieMap[(Int,Int),Edge]()  // Map of Edges contained in the partition

  val printing = false                  // should the handled messages be printed to terminal
  val logging  = false                  // should the state of the vertex/edge map be output to file

  var messageCount          = 0         // number of messages processed since last report to the benchmarker
  var secondaryMessageCount = 0
  var messageBlockID        = 0         // id of current message block to syncronise times across partition managers
  val secondaryCounting     = false     // count all messages or just main incoming ones

  val mediator              = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  mediator ! DistributedPubSubMediator.Put(self)

  val entitiesGauge         = Kamon.gauge("raphtory.entities")
  val verticesGauge         = Kamon.gauge("raphtory.vertices")
  val edgesGauge            = Kamon.gauge("raphtory.edges")

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
    case "tick" => reportIntake()
    case "profile" => profile()
    case "keep_alive" => keepAlive()

    //case LiveAnalysis(name,analyser) => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)

    case VertexAdd(msgId,srcId) => Task.eval(vertexAdd(msgId,srcId)).runAsync; vHandle(srcId)
    case VertexAddWithProperties(msgId,srcId,properties) => vertexAdd(msgId,srcId,properties); vHandle(srcId);
    case VertexRemoval(msgId,srcId) => vertexRemoval(msgId,srcId); vHandle(srcId);

    case EdgeAdd(msgId,srcId,dstId) => Task.eval(edgeAdd(msgId,srcId,dstId)).runAsync; eHandle(srcId,dstId)
    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAdd(msgId,srcId,dstId,properties); eHandle(srcId,dstId)
    case RemoteEdgeAdd(msgId,srcId,dstId,properties) => remoteEdgeAdd(msgId,srcId,dstId,properties); eHandleSecondary(srcId,dstId)
    case RemoteEdgeAddNew(msgId,srcId,dstId,properties,deaths) => remoteEdgeAddNew(msgId,srcId,dstId,properties,deaths); eHandleSecondary(srcId,dstId)

    case EdgeRemoval(msgId,srcId,dstId) => edgeRemoval(msgId,srcId,dstId); eHandle(srcId,dstId)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId); eHandleSecondary(srcId,dstId)
    case RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths) => remoteEdgeRemovalNew(msgId,srcId,dstId,deaths); eHandleSecondary(srcId,dstId)

    case RemoteReturnDeaths(msgId,srcId,dstId,deaths) => remoteReturnDeaths(msgId,srcId,dstId,deaths); eHandleSecondary(srcId,dstId)
    case ReturnEdgeRemoval(msgId,srcId,dstId) => returnEdgeRemoval(msgId,srcId,dstId);  eHandleSecondary(srcId,dstId)

    case UpdatedCounter(newValue) => {
      managerCount = newValue
      println(s"Maybe a new PartitionManager has arrived: ${newValue}")
    }
  }

  def vertexAdd(msgId : Int, srcId : Int, properties : Map[String,String] = null) : Vertex = { //Vertex add handler function
    var value : Vertex = new Vertex(msgId, srcId, true)
    vertices.putIfAbsent(srcId, value) match {
      case Some(oldValue) => {
        oldValue revive msgId
        value = oldValue
      }
      case None =>
    }
    if (properties != null)
      properties.foreach(l => value + (msgId,l._1,l._2)) //add all properties
    value
  }

  def edgeAdd(msgId : Int, srcId : Int, dstId : Int, properties : Map[String, String] = null) : Unit = {
    //val (edge, local, present) = edgeCreator(msgId, srcId, dstId, managerCount, managerID, edges)

    val local       = checkDst(dstId, managerCount, managerID)
    var present     = false
    var edge : Edge = null

    if (local)
      edge = new Edge(msgId, true, srcId, dstId)
    else
      edge = new RemoteEdge(msgId, true, srcId, dstId, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.putIfAbsent((srcId, dstId), edge) match {
      case Some(e) => {
        edge = e
        present = true
      }
      case None => // All is set
    }
    if (printing) println(s"Received an edge Add for $srcId --> $dstId (local: $local)")

    if (local && srcId != dstId) {
      val dstVertex = vertexAdd(msgId, dstId) // do the same for the destination ID
      dstVertex addAssociatedEdge edge // do the same for the destination node
      if (!present)
        edge killList dstVertex.removeList
    }

    val srcVertex = vertexAdd(msgId, srcId)                          // create or revive the source ID
    srcVertex addAssociatedEdge edge // add the edge to the associated edges of the source node

    if (present) {
        edge revive msgId
        if (!local)
          mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAdd(msgId, srcId, dstId, null),false) // inform the partition dealing with the destination node*/
    } else {
        val deaths = srcVertex.removeList
        edge killList deaths
        if (!local)
          mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAddNew(msgId, srcId, dstId, null, deaths), false)
    }
    if (properties != null)
      properties.foreach(prop => edge + (msgId,prop._1,prop._2)) // add all passed properties onto the edge

  }

  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String] = null):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge already exists so just updating")
    vertexAdd(msgId,dstId) //create or revive the destination node
    val edge = edges((srcId, dstId))
    vertices(dstId) addAssociatedEdge edge //again I think this can be removed
    edge revive msgId //revive  the edge
    if (properties != null)
      properties.foreach(prop => edge + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }

  def remoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:mutable.TreeMap[Int, Boolean]):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge did not previously exist so sending back deaths")
    vertexAdd(msgId,dstId) //create or revive the destination node
    val edge = new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId, managerCount))
    vertices(dstId) addAssociatedEdge edge //add the edge to the associated edges of the destination node
    edges put((srcId,dstId), edge) //create the new edge
    val deaths = vertices(dstId).removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    properties.foreach(prop => edge + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def getVertexAndWipe(id : Int, msgId : Int) : Vertex = {
    vertices.get(id) match {
      case Some(value) => value
      case None => {
        val x  = new Vertex(msgId,id,true)
        vertices put(id, x)
        x wipe()
        x
      }
    }
  }

  def edgeRemoval(msgId:Int, srcId:Int, dstId:Int):Unit={
    val (edge, local, present) = edgeCreator(msgId, srcId, dstId, managerCount, managerID, edges, false)
    if (printing) println(s"Received an edge Add for $srcId --> $dstId (local: $local)")
    var dstVertex : Vertex = null
    var srcVertex : Vertex = null


    if (local && srcId != dstId) {
      dstVertex = getVertexAndWipe(dstId, msgId)
      dstVertex addAssociatedEdge edge // do the same for the destination node
      if (!present)
        edge killList dstVertex.removeList
    }
    srcVertex = getVertexAndWipe(srcId, msgId)
    srcVertex addAssociatedEdge edge // add the edge to the associated edges of the source node

    if (present) {
        edge kill msgId
        if (!local)
          mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemoval(msgId,srcId,dstId),false) // inform the partition dealing with the destination node
    } else {
        val deaths = srcVertex.removeList
        edge killList deaths
        if (!local)
          mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths), false)
    }
  }


  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge already exists so just updating")
    var dstVertex = getVertexAndWipe(dstId, msgId)
    (srcId,dstId) //add the edge to the destination nodes associated list
    edges.get(srcId, dstId) match {
      case Some(e) => {
        e kill msgId
        dstVertex addAssociatedEdge e
      }
      case None    => println("Didn't exist") //possibly need to fix when adding the priority box
    }
  }

  def remoteEdgeRemovalNew(msgId:Int,srcId:Int,dstId:Int,srcDeaths:mutable.TreeMap[Int, Boolean]):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge did not previously exist so sending back deaths ")
    val dstVertex = getVertexAndWipe(dstId, msgId)
    val edge = new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Source,getPartition(srcId, managerCount))
    dstVertex addAssociatedEdge edge  //add the edge to the destination nodes associated list
    edges put((srcId,dstId), edge) // otherwise create and initialise as false

    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    if (printing) println(s"Received vertex remove for $srcId, updating + informing all edges")
    var vertex : Vertex = null
    vertices.get(srcId) match {
      case Some(v) => {
        vertex = v
        v kill msgId
      }
      case None    => {
        vertex = new Vertex(msgId, srcId, false)
        vertices put (srcId, vertex)
      }
    }

    vertices(srcId).associatedEdges.foreach(e => {
      e kill msgId
      if(e.isInstanceOf[RemoteEdge]){
        val ee = e.asInstanceOf[RemoteEdge]
        if(ee.remotePos == RemotePos.Destination) {
          mediator ! DistributedPubSubMediator.Send(getManager(ee.remotePartitionID, managerCount), RemoteEdgeRemoval(msgId, ee.srcId, ee.dstId),false)
        } //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
        else{
          mediator ! DistributedPubSubMediator.Send(getManager(ee.remotePartitionID, managerCount), ReturnEdgeRemoval(msgId, ee.srcId, ee.dstId), false)
        }//This is the case if the remote vertex is the source of the edge. In this case we handle it with the specialised function below
      }
    })
  }

  def returnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal (return) for $srcId --> $dstId from ${getManager(dstId, managerCount )}. Edge already exists so just updating")
    val srcVertex = getVertexAndWipe(srcId, msgId)
    val edge = edges(srcId, dstId)

    srcVertex addAssociatedEdge edge //add the edge to the destination nodes associated list
    edge kill msgId                  // if the edge already exists, kill it
  }

  def remoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,dstDeaths:mutable.TreeMap[Int, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    edges(srcId,dstId) killList dstDeaths
  }

  //***************** EDGE HELPERS


  /********************
   *    PRINT BLOCK   *
   ********************/
  def printToFile(entityName:String,msg: String):Unit={
    val file = new File(s"/logs/${self.path.name}/entityLogs")
    file.mkdirs()
    val fw:FileWriter = new FileWriter(s"/logs/${self.path.name}/entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }

  def log(srcId:Int):Unit = if(logging && (vertices contains srcId)) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())

  def log(srcId:Int,dstId:Int):Unit={
    if(logging && (edges contains (srcId,dstId))) {
      if(edges(srcId,dstId).isInstanceOf[RemoteEdge]) {
        if(edges(srcId,dstId).asInstanceOf[RemoteEdge].remotePos == RemotePos.Source) printToFile(s"RemoteEdge$srcId-->$dstId",s"${edges(srcId,dstId).printHistory()}")
        else printToFile(s"Edge$srcId-->$dstId",s"${edges(srcId,dstId).printHistory()}")
      }
      else printToFile(s"Edge$srcId-->$dstId",s"${edges(srcId,dstId).printHistory()}")
    }
  }

  /********************
   * .END PRINT BLOCK *
   ********************/

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

    getGauge("Total number of entities").set(map.size)
    //getGauge("Total number of properties") TODO

    getGauge("Total number of previous states").set(
      getEntitiesPrevStates(map)
    )

    // getGauge("Number of props previous history") TODO
  }

  def reportIntake() : Unit = {
    if(printing)
      println(messageCount)
    // Kamon monitoring
    kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount").set(messageCount)
    kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount").set(secondaryMessageCount)
    reportSizes(edgesGauge, edges)
    reportSizes(verticesGauge, vertices)
    // Heap benchmarking
    //profile()

    // Set counters
    messageCount          = 0
    secondaryMessageCount = 0
    messageBlockID        = messageBlockID + 1
  }

  def keepAlive() = {
    mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(managerID), false)
  }

  def vHandle(srcID : Int) : Unit = {
    messageCount += 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "messageCounter").increment()
  }

  def vHandleSecondary(srcID : Int) : Unit = {
    secondaryMessageCount += 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCounter").increment()
  }
  def eHandle(srcID : Int, dstID : Int) : Unit = {
    messageCount += 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "messageCounter").increment()
  }

  def eHandleSecondary(srcID : Int, dstID : Int) : Unit = {
    secondaryMessageCount += 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCounter").increment()
  }

  /*********************************
   * END Metrics reporting methods *
   *********************************/
}
