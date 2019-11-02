package com.raphtory.core.storage

import java.util
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Property, RemoteEdge, RemotePos, Vertex}
import com.raphtory.core.utils.{EntityRemovedAtTimeException, PushedOutOfGraphException, StillWithinLiveGraphException, Utils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO What happens when an edge which has been archived gets readded

class EntityStorage(workerID:Int) {
  import com.raphtory.core.utils.Utils.{checkDst, getPartition, getManager,checkWorker}

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
  /**
    * Vertices Methods
    */

  def vertexAdd(routerID:Int,msgTime : Long, srcId : Long, properties : Map[String,String] = null) : Vertex = { //Vertex add handler function
    var value : Vertex = null
    vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => { //if it does
        v revive msgTime //add the history point
        v updateLatestRouter routerID // record the latest router for windowing
        value = v
      }
      case None => { //if it does not exist
        value = new Vertex(routerID,msgTime, srcId, initialValue = true,this) //create a new vertex
        vertices put(srcId, value) //put it in the map
      }
    }
    if (properties != null) { //if the add come with some properties
      properties.foreach(prop => value.updateProp(prop._1, new Property(msgTime, prop._1, prop._2,this))) // add all passed properties into the vertex
    }
    value //return the vertex
  }

  def vertexWorkerRequest(routerID:Int, msgTime:Long, dstID:Long, srcID:Long, edge:Edge, present:Boolean) ={
    val dstVertex = vertexAdd(routerID,msgTime, dstID) //if the worker creating an edge does not deal with the destination
    if (!present) {
      dstVertex addIncomingEdge (edge) // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send(getManager(srcID, managerCount), DstResponseFromOtherWorker(routerID,msgTime,srcID, dstID, dstVertex.removeList), false)
    }
  }

  def vertexWipeWorkerRequest(routerID:Int, msgTime:Long, dstID:Long, srcID:Long, edge:Edge, present:Boolean) ={
    val dstVertex =getVertexOrPlaceholder(routerID,dstID, msgTime) // if the worker creating an edge does not deal with do the same for the destination ID
    if (!present) {
      dstVertex addIncomingEdge (edge) // do the same for the destination node
      mediator ! DistributedPubSubMediator.Send(getManager(srcID, managerCount), DstResponseFromOtherWorker(routerID,msgTime,srcID, dstID, dstVertex.removeList), false)
    }
  }

  def vertexWorkerRequestEdgeHandler(routerID:Int,msgTime:Long,srcID:Long, dstID:Long, removeList: mutable.TreeMap[Long, Boolean]): Unit ={
      getVertexOrPlaceholder(routerID,srcID,msgTime).getOutgoingEdge(dstID) match {
      case Some(edge) => edge killList removeList //add the dst removes into the edge
      case None => println("Oh no")
    }
  }

  def getVertexOrPlaceholder(routerID : Int, id : Long, msgTime : Long) : Vertex = {
    vertices.get(id) match {
      case Some(vertex) => {
        vertex
      }
      case None => {
        val vertex  = new Vertex(routerID, msgTime,id,initialValue = true,this)
        vertices put(id, vertex)
        vertex wipe()
        //newVertexKey(workerID,id) //and record the new key
        vertex
      }
    }
  }

  def vertexRemoval(routerID:Int,msgTime:Long,srcId:Long) : Unit = {
    var vertex : Vertex = null
    vertices.get(srcId) match {
      case Some(v) => {
        if(windowing) {
          if (!(v latestRouterCheck routerID)) {//if we are windowing we must check the latest Router for the vertex
            return //if its not from the same router we ignore and return the function here
          }
        }
        vertex = v
        v kill msgTime //if we are not windowing we just run as normal or if it is the correct router we remove
      }
      case None => { //if the removal has arrived before the creation
        vertex = new Vertex(routerID,msgTime, srcId, initialValue = false,this) //create a placeholder
        vertices put(srcId, vertex) //add it to the map
      }
    }
    vertex.incomingEdges.foreach(e =>{ //incoming edges are not handled by this worker as the source vertex is in charge
          val edge = e._2
          edge match {
            case remoteEdge: RemoteEdge =>
              edge kill msgTime
              mediator ! DistributedPubSubMediator.Send(getManager(remoteEdge.getSrcId, managerCount), ReturnEdgeRemoval(routerID, msgTime, remoteEdge.srcId, remoteEdge.dstId), false) //inform the other partition to do the same
            case _ => //if it is a local edge -- opperated by the same worker, therefore we can perform an action -- otherwise we must inform the other local worker to handle this
              if (edge.getWorkerID == workerID) edge kill msgTime
              else mediator ! DistributedPubSubMediator.Send(getManager(edge.getSrcId, managerCount), EdgeRemoveForOtherWorker(routerID, msgTime, edge.getSrcId, edge.getDstId), false) //
          }
    })
    vertex.outgoingEdges.foreach(e =>{
      val edge = e._2
      edge kill msgTime//outgoing edge always opperated by the same worker, therefore we can perform an action
      edge match {
        case remoteEdge: RemoteEdge =>
          mediator ! DistributedPubSubMediator.Send(getManager(edge.getDstId, managerCount), RemoteEdgeRemoval(routerID, msgTime, remoteEdge.srcId, remoteEdge.dstId), false)
        case _ => //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
      }
    })
  }

  /**
    * Edges Methods
    */
  def edgeAdd(routerID:Int,msgTime : Long, srcId : Long, dstId : Long, properties : Map[String, String] = null) = {
    val local       = checkDst(dstId, managerCount, managerID) //is the dst on this machine
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker
    var present     = false //if the vertex is new or not -- decides what update is sent when remote and if to add the source/destination removals

    val srcVertex = vertexAdd(routerID,msgTime, srcId) // create or revive the source ID
    var edge : Edge = null
    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => { //retrieve the edge if it exists
        edge = e
        present = true
      }
      case None => //if it does not
        if (local)
          edge = new Edge(routerID,workerID,msgTime, srcId, dstId, initialValue = true,this) //create the new edge, local or remote
        else
          edge = new RemoteEdge(routerID,workerID,msgTime, srcId, dstId, initialValue = true, RemotePos.Destination, getPartition(dstId, managerCount),this)
        srcVertex.addOutgoingEdge(edge) //add this edge to the vertex
    }
    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = vertexAdd(routerID,msgTime, dstId) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge(edge) // add it to the dst as would not have been seen
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      }
      else // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstAddForOtherWorker(routerID,msgTime,dstId,srcId,edge,present),true)
    }

    if (present) {
      edge revive msgTime //if the edge was previously created we need to revive it
      edge updateLatestRouter routerID //and update its latest router for windowing
      if (!local) // if it is a remote edge we
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAdd(routerID,msgTime, srcId, dstId, properties),false) // inform the partition dealing with the destination node*/
    } else { // if this is the first time we have seen the edge
      val deaths = srcVertex.removeList //we extract the removals from the src
      edge killList deaths // add them to the edge
      if (!local) // and if not local sync with the other partition
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAddNew(routerID,msgTime, srcId, dstId, properties, deaths), false)
    }
    if (properties != null) properties.foreach(prop => edge.updateProp(prop._1, new Property(msgTime, prop._1, prop._2,this))) // add all passed properties onto the edge

  }

  def remoteEdgeAddNew(routerID : Int, msgTime:Long,srcId:Long,dstId:Long,properties:Map[String,String],srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = vertexAdd(routerID,msgTime,dstId) //create or revive the destination node
    val edge = new RemoteEdge(routerID, workerID, msgTime, srcId, dstId, initialValue = true,RemotePos.Source,getPartition(srcId, managerCount),this)
    dstVertex addIncomingEdge(edge) //add the edge to the associated edges of the destination node
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    if (properties != null)
      properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(routerID,msgTime,srcId,dstId,deaths),false)
  }

  def remoteEdgeAdd(routerID : Int, msgTime:Long,srcId:Long,dstId:Long,properties:Map[String,String] = null):Unit={
    val dstVertex = vertexAdd(routerID,msgTime,dstId) // revive the destination node
    dstVertex.getIncomingEdge(srcId) match {
      case Some(edge) => {
        edge updateLatestRouter routerID
        edge revive msgTime //revive the edge
        if (properties != null)
          properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
      }
      case None =>{/*todo should this happen */}
    }
  }

  def edgeRemoval(routerID : Int, msgTime:Long, srcId:Long, dstId:Long):Unit={
    val local       = checkDst(dstId, managerCount, managerID)
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker

    var present     = false
    var edge : Edge = null
    var srcVertex : Vertex  = getVertexOrPlaceholder(routerID,srcId,msgTime)

    srcVertex.getOutgoingEdge(dstId) match {
      case Some(e) => {
        edge = e
        present = true
        if (windowing) {
          if (!(edge latestRouterCheck routerID)) { //if we are windowing we must check the latest Router for the vertex
            return //if its not from the same router we ignore and return the function here
          }
        }
      }
      case None => {
        if (local)
          edge = new Edge(routerID,workerID, msgTime, srcId, dstId, initialValue = false,this)
        else
          edge = new RemoteEdge(routerID, workerID, msgTime,srcId, dstId, initialValue = false, RemotePos.Destination, getPartition(dstId, managerCount),this)
        srcVertex addOutgoingEdge(edge) // add the edge to the associated edges of the source node
      }
    }
    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = getVertexOrPlaceholder(routerID,dstId,msgTime) // do the same for the destination ID
        if (!present) {
          dstVertex addIncomingEdge(edge) // do the same for the destination node
          edge killList dstVertex.removeList //add the dst removes into the edge
        }
      }
      else{ // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstWipeForOtherWorker(routerID,msgTime,dstId,srcId,edge,present),true)
      }
    }

    if (present) {
      edge kill msgTime
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemoval(routerID,msgTime,srcId,dstId),false) // inform the partition dealing with the destination node
    }
    else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths), false)
    }
  }

  def returnEdgeRemoval(routerID:Int,msgTime:Long,srcId:Long,dstId:Long):Unit={ //for the source getting an update abou
    getVertexOrPlaceholder(routerID,srcId,msgTime).getOutgoingEdge(dstId) match {
      case Some(edge)=> edge kill msgTime
      case None => //todo should this happen
    }
  }

  def edgeRemovalFromOtherWorker(routerID:Int,msgTime:Long,srcID:Long,dstID:Long) = {
    getVertexOrPlaceholder(routerID,srcID, msgTime).getOutgoingEdge(dstID) match {
      case Some(edge) => edge kill msgTime
      case None => //todo should this happen?
    }
  }

  def remoteEdgeRemoval(routerID : Int,msgTime:Long,srcId:Long,dstId:Long):Unit={
    val dstVertex = getVertexOrPlaceholder(routerID,dstId, msgTime)
    dstVertex.getIncomingEdge(srcId) match {
      case Some(e) => e kill msgTime
      case None    => println(s"Worker ID $workerID Manager ID $managerID")
    }
  }

  def remoteEdgeRemovalNew(routerID : Int,msgTime:Long,srcId:Long,dstId:Long,srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = getVertexOrPlaceholder(routerID,dstId, msgTime)
    val edge = new RemoteEdge(routerID,workerID,msgTime,srcId, dstId, initialValue = false, RemotePos.Source, getPartition(srcId, managerCount),this)
    dstVertex addIncomingEdge(edge)  //add the edge to the destination nodes associated list
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(routerID,msgTime,srcId,dstId,deaths),false)
  }

  def remoteReturnDeaths(routerID:Int,msgTime:Long,srcId:Long,dstId:Long,dstDeaths:mutable.TreeMap[Long, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    getVertexOrPlaceholder(routerID,srcId,msgTime).getOutgoingEdge(dstId) match {
      case Some(edge) => edge killList dstDeaths
      case None => /*todo Should this happen*/
    }
  }
}


