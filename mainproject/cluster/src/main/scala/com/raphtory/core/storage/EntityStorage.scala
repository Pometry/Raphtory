package com.raphtory.core.storage

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Entity, SplitEdge, Vertex}
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO What happens when an edge which has been archived gets readded

class EntityStorage(workerID:Int) {
  import com.raphtory.core.utils.Utils.{checkDst, checkWorker, getManager, getPartition}
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean

  var messageCount                = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)       // number of messages processed since last report to the benchmarker
  var secondaryMessageCount       = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)
  var workerMessageCount          = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)

  var vertexDeletionCount         = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)
  var vertexHistoryDeletionCount  = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)
  var vertexPropertyDeletionCount = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)

  var edgeDeletionCount           = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)
  var edgeHistoryDeletionCount    = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)
  var edgePropertyDeletionCount   = ArrayBuffer[Int](0,0,0,0,0,0,0,0,0,0)

  /**
    * Map of vertices contained in the partition
    */
  val vertices  = ParTrieMap[Long, Vertex]()

  var printing          : Boolean     = true
  var managerCount      : Int         = 1
  var managerID         : Int         = 0
  var mediator          : ActorRef    = null
  var windowing         : Boolean     =  Utils.windowing
  //stuff for compression and archiving
  var oldestTime        :Long         = Long.MaxValue
  var newestTime        :Long         = Long.MinValue
  var lastCompressedAt  :Long         = 0

  def timings(updateTime:Long) ={
    if (updateTime < oldestTime && updateTime >0) oldestTime=updateTime
    if(updateTime > newestTime) newestTime = updateTime //this isn't thread safe, but is only an approx for the archiving
  }

  def apply(printing : Boolean, managerCount : Int, managerID : Int, mediator : ActorRef) = {
    this.printing     = printing
    this.managerCount = managerCount
    this.managerID    = managerID
    this.mediator     = mediator
    this
  }

  def setManagerCount(count : Int) = this.managerCount = count

  def addProperties(msgTime:Long,entity:Entity,properties:Properties) = {
    if (properties != null) {
      properties.property.foreach {
        case StringProperty(key, value) => entity + (msgTime, false,key, value)
        case LongProperty(key, value) => entity + (msgTime,false, key, value)
        case DoubleProperty(key, value) => entity + (msgTime,false, key, value)
        case ImmutableProperty(key, value) => entity + (msgTime,true, key, value)
      }
    }
  } // if the add come with some properties add all passed properties into the entity


  def vertexAdd(msgTime : Long, srcId : Long, properties : Properties = null) : Vertex = { //Vertex add handler function
    //if(debug)println(s"Adding $srcId")
    val vertex : Vertex = vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => { //if it does
        v revive msgTime //add the history point
        v
      }
      case None => { //if it does not exist
        val v = new Vertex(msgTime, srcId, initialValue = true,storage=this) //create a new vertex
        vertices put(srcId, v) //put it in the map
        v
      }
    }
    addProperties(msgTime,vertex,properties)
    vertex //return the vertex
  }

  def getVertexOrPlaceholder(msgTime : Long,id : Long) : Vertex = {
    vertices.get(id) match {
      case Some(vertex) => vertex
      case None =>
        val vertex  = new Vertex(msgTime,id,initialValue = true,this)
        vertices put(id, vertex)
        vertex wipe()
        vertex
    }
  }


  def vertexWorkerRequest(msgTime:Long, dstID:Long, srcID:Long, edge:Edge, present:Boolean) ={
    val dstVertex = vertexAdd(msgTime, dstID) //if the worker creating an edge does not deal with the destination
    if (!present) {
      dstVertex addIncomingEdge edge // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send(getManager(srcID, managerCount), DstResponseFromOtherWorker(msgTime,srcID, dstID, dstVertex.removeList), false)
    }
  }

  def vertexWipeWorkerRequest(msgTime:Long, dstID:Long, srcID:Long, edge:Edge, present:Boolean) ={
    val dstVertex =getVertexOrPlaceholder(msgTime,dstID) // if the worker creating an edge does not deal with do the same for the destination ID
    if (!present) {
      dstVertex addIncomingEdge edge // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send(getManager(srcID, managerCount), DstResponseFromOtherWorker(msgTime,srcID, dstID, dstVertex.removeList), false)
    }
  }

  def vertexWorkerRequestEdgeHandler(msgTime:Long,srcID:Long, dstID:Long, removeList: mutable.TreeMap[Long, Boolean]): Unit ={
      getVertexOrPlaceholder(msgTime,srcID).getOutgoingEdge(dstID) match {
      case Some(edge) => edge killList removeList //add the dst removes into the edge
      case None => println("Oh no")
    }
  }


  def vertexRemoval(msgTime:Long,srcId:Long) : Unit = {
    val vertex : Vertex = vertices.get(srcId) match {
      case Some(v) => {
        v kill msgTime
        v
      }
      case None => { //if the removal has arrived before the creation
        val v = new Vertex(msgTime, srcId, initialValue = false,this) //create a placeholder
        vertices put(srcId, v) //add it to the map
        v
      }
    }
    vertex.incomingEdges.values.foreach {
      case edge@(remoteEdge: SplitEdge) =>
        edge kill msgTime
        mediator ! DistributedPubSubMediator.Send(getManager(remoteEdge.getSrcId, managerCount), ReturnEdgeRemoval(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId), false) //inform the other partition to do the same
      case edge => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
        if (edge.getWorkerID == workerID) edge kill msgTime
        else mediator ! DistributedPubSubMediator.Send(getManager(edge.getSrcId, managerCount), EdgeRemoveForOtherWorker(msgTime, edge.getSrcId, edge.getDstId), false) //
    }
    vertex.outgoingEdges.values.foreach {
      case edge@(remoteEdge: SplitEdge) =>
        edge kill msgTime//outgoing edge always opperated by the same worker, therefore we can perform an action
        mediator ! DistributedPubSubMediator.Send(getManager(edge.getDstId, managerCount), RemoteEdgeRemoval(msgTime, remoteEdge.getSrcId, remoteEdge.getDstId), false)
      case edge => edge kill msgTime //outgoing edge always opperated by the same worker, therefore we can perform an action
    }
  }

  /**
    * Edges Methods
    */
  def edgeAdd(msgTime : Long, srcId : Long, dstId : Long, properties : Properties = null) = {
    val local       = checkDst(dstId, managerCount, managerID) //is the dst on this machine
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker
    val srcVertex = vertexAdd(msgTime, srcId) // create or revive the source ID

    var present     = false //if the vertex is new or not -- decides what update is sent when remote and if to add the source/destination removals
    var edge : Edge = null
    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) =>  //retrieve the edge if it exists
        edge = e
        present = true
      case None => //if it does not
        if (local) edge = new Edge(workerID,msgTime, srcId, dstId, initialValue = true,this) //create the new edge, local or remote
        else edge = new SplitEdge(workerID,msgTime, srcId, dstId, initialValue = true, getPartition(dstId, managerCount),this)
        srcVertex.addOutgoingEdge(edge) //add this edge to the vertex
    }
    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = vertexAdd(msgTime, dstId) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge(edge) // add it to the dst as would not have been seen
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      }
      else // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstAddForOtherWorker(msgTime,dstId,srcId,edge,present),true)
    }

    if (present) {
      edge revive msgTime //if the edge was previously created we need to revive it
      if (!local) // if it is a remote edge we
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAdd(msgTime, srcId, dstId, properties),false) // inform the partition dealing with the destination node*/
    } else { // if this is the first time we have seen the edge
      val deaths = srcVertex.removeList //we extract the removals from the src
      edge killList deaths // add them to the edge
      if (!local) // and if not local sync with the other partition
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAddNew(msgTime, srcId, dstId, properties, deaths), false)
    }
    addProperties(msgTime,edge,properties)
  }

  def remoteEdgeAddNew(msgTime:Long,srcId:Long,dstId:Long,properties:Properties,srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = vertexAdd(msgTime,dstId) //create or revive the destination node
    val edge = new SplitEdge( workerID, msgTime, srcId, dstId, initialValue = true,getPartition(srcId, managerCount),this)
    dstVertex addIncomingEdge(edge) //add the edge to the associated edges of the destination node
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    addProperties(msgTime,edge,properties)
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }

  def remoteEdgeAdd(msgTime:Long,srcId:Long,dstId:Long,properties:Properties = null):Unit={
    val dstVertex = vertexAdd(msgTime,dstId) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) => {
        edge revive msgTime //revive the edge
        addProperties(msgTime,edge,properties)
      }
      case None =>{/*todo should this happen */}
    }
  }

  def edgeRemoval(msgTime:Long, srcId:Long, dstId:Long):Unit={
    val local       = checkDst(dstId, managerCount, managerID)
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker

    var present     = false
    var edge : Edge = null
    var srcVertex : Vertex  = getVertexOrPlaceholder(msgTime,srcId)

    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => {
        edge = e
        present = true
      }
      case None => {
        if (local)
          edge = new Edge(workerID, msgTime, srcId, dstId, initialValue = false,this)
        else
          edge = new SplitEdge(workerID, msgTime,srcId, dstId, initialValue = false, getPartition(dstId, managerCount),this)
        srcVertex addOutgoingEdge(edge) // add the edge to the associated edges of the source node
      }
    }
    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = getVertexOrPlaceholder(msgTime,dstId) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge(edge) // do the same for the destination node
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      }
      else{ // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstWipeForOtherWorker(msgTime,dstId,srcId,edge,present),true)
      }
    }

    if (present) {
      edge kill msgTime
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemoval(msgTime,srcId,dstId),false) // inform the partition dealing with the destination node
    }
    else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemovalNew(msgTime,srcId,dstId,deaths), false)
    }
  }

  def returnEdgeRemoval(msgTime:Long,srcId:Long,dstId:Long):Unit={ //for the source getting an update abou
    getVertexOrPlaceholder(msgTime,srcId).getOutgoingEdge(dstId) match {
      case Some(edge)=> edge kill msgTime
      case None => //todo should this happen
    }
  }

  def edgeRemovalFromOtherWorker(msgTime:Long,srcID:Long,dstID:Long) = {
    getVertexOrPlaceholder(msgTime,srcID).getOutgoingEdge(dstID) match {
      case Some(edge) => edge kill msgTime
      case None => //todo should this happen?
    }
  }

  def remoteEdgeRemoval(msgTime:Long,srcId:Long,dstId:Long):Unit={
    val dstVertex = getVertexOrPlaceholder(msgTime,dstId)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => println(s"Worker ID $workerID Manager ID $managerID")
    }
  }

  def remoteEdgeRemovalNew(msgTime:Long,srcId:Long,dstId:Long,srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = getVertexOrPlaceholder(msgTime,dstId)
    val edge = new SplitEdge(workerID,msgTime,srcId, dstId, initialValue = false, getPartition(srcId, managerCount),this)
    dstVertex addIncomingEdge(edge)  //add the edge to the destination nodes associated list
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }

  def remoteReturnDeaths(msgTime:Long,srcId:Long,dstId:Long,dstDeaths:mutable.TreeMap[Long, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    getVertexOrPlaceholder(msgTime,srcId).getOutgoingEdge(dstId) match {
      case Some(edge) => edge killList dstDeaths
      case None => /*todo Should this happen*/
    }
  }
}


