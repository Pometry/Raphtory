package com.raphtory.core.storage

import java.util.NoSuchElementException

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator

import scala.collection.concurrent.TrieMap
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Property, RemoteEdge, RemotePos, Vertex}
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO check which percentage of updates are compressed/archived
//TODO make compression and archivist based on relative 1st update to latest update time
//TODO keep cached entities in a separate map which can be cleared once analysis is finished
//TODO filter to set of entities, expand to x number of hop neighbours and retrieve history
//TODO perhaps create a new map for each LAM, this way we can add the entities to this map and remove after
//TODO workout what to do sub millisecond as currently overwrites
//TODO retrieve edge on other partition manager -- decide how to propagate
//TODO do we need an edge map?
object EntityStorage {
  import com.raphtory.core.utils.Utils.{checkDst, getEdgeIndex, getPartition, getManager}
  /**
    * Map of vertices contained in the partition
    */
  val vertices = ParTrieMap[Int, Vertex]()

  /**
    * Map of edges contained in the partition
    */
  val edges    = ParTrieMap[Long, Edge]()  // Map of Edges contained in the partition

  var printing     : Boolean = false
  var managerCount : Int     = 1
  var managerID    : Int     = 0
  var mediator     : ActorRef= null
  var addOnlyVertex      : Boolean =  System.getenv().getOrDefault("ADD_ONLY_VERTEX", "false").trim.toBoolean
  var addOnlyEdge      : Boolean =  System.getenv().getOrDefault("ADD_ONLY_EDGE", "false").trim.toBoolean
  var windowing        : Boolean =  System.getenv().getOrDefault("WINDOWING", "false").trim.toBoolean


  def apply(printing : Boolean, managerCount : Int, managerID : Int, mediator : ActorRef) = {
    this.printing     = printing
    this.managerCount = managerCount
    this.managerID    = managerID
    this.mediator     = mediator
    this
  }

  def setManagerCount(count : Int) = {
    this.managerCount = count
  }

  /**
    * Vertices Methods
    */

  def vertexAdd(routerID:Int,msgTime : Long, srcId : Int, properties : Map[String,String] = null) : Vertex = { //Vertex add handler function
    if(printing) println(s"Received vertex add for $srcId with map: $properties")
    var value : Vertex = new Vertex(routerID,msgTime, srcId, initialValue = true, addOnlyVertex)
    vertices.synchronized {
      vertices.get(srcId) match {
        case Some(v) =>
          v revive msgTime
          v updateLatestRouter routerID
          value = v
        case None =>
          vertices put(srcId, value)
      }
    }

    if (properties != null) {
      properties.foreach(prop => value.updateProp(prop._1, new Property(msgTime, prop._1, prop._2))) // add all passed properties onto the edge
    }
    //properties.foreach(l => value + (msgTime,l._1,l._2)) //add all properties
    GraphRepoProxy.addVertex(value.getId)
    value
  }

  def vertexRemoval(routerID:Int,msgTime:Long,srcId:Int) : Unit = {
    if (printing) println(s"Received vertex remove for $srcId, updating + informing all edges")
    var vertex : Vertex = null
    vertices.synchronized {
      vertices.get(srcId) match {
        case Some(v) => {
          vertex = v
          if(windowing) {
            if (!(v latestRouterCheck routerID)) {
              //if we are windowing we must check the latest Router for the vertex
              return //if its not from the same router we ignore and return the function here
            }
          }
          v kill msgTime //if we are not windowing we just run as normal or if it is the correct router we remove
        }
        case None => {
          vertex = new Vertex(routerID,msgTime, srcId, initialValue = false, addOnly = addOnlyVertex)
          vertices put(srcId, vertex)
        }
      }
    }

    vertex.associatedEdges.values.foreach(e => {
      e kill msgTime
      try {
        val ee = e.asInstanceOf[RemoteEdge]
        if (ee.remotePos == RemotePos.Destination) {
          mediator ! DistributedPubSubMediator.Send(getManager(ee.remotePartitionID, managerCount), RemoteEdgeRemoval(routerID,msgTime, ee.srcId, ee.dstId), false)
        } //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
        else {
          mediator ! DistributedPubSubMediator.Send(getManager(ee.remotePartitionID, managerCount), ReturnEdgeRemoval(routerID,msgTime, ee.srcId, ee.dstId), false)
        } //This is the case if the remote vertex is the source of the edge. In this case we handle it with the specialised function below

      } catch {
        case _ : ClassCastException =>
      }
    })
  }
  //TODO: does the routerID effect vertices which are wiped
  def getVertexAndWipe(routerID : Int, id : Int, msgTime : Long) : Vertex = {
    vertices.get(id) match {
      case Some(value) => value
      case None => {
        val x  = new Vertex(routerID, msgTime,id,initialValue = true, addOnlyVertex)
        vertices put(id, x)
        x wipe()
        x
      }
    }
  }

  /**
    * Edges Methods
    */
  def edgeAdd(routerID:Int,msgTime : Long, srcId : Int, dstId : Int, properties : Map[String, String] = null) = {
    val local       = checkDst(dstId, managerCount, managerID)
    var present     = false //if the vertex is new or not -- decides what update is sent when remote and if to add the source/destination removals
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)
    if (local)
      edge = new Edge(routerID,msgTime, srcId, dstId, initialValue = true, addOnlyEdge)
    else
      edge = new RemoteEdge(routerID,msgTime, srcId, dstId, initialValue = true, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.synchronized {
      edges.get(index) match {
        case Some(e) => {
          edge = e
          present = true
        }
        case None =>
          edges.put(index, edge)
      }
    }

    if (printing) println(s"Received an edge Add for $srcId --> $dstId (local: $local)")

    if (local && srcId != dstId) {
      val dstVertex = vertexAdd(routerID,msgTime, dstId) // do the same for the destination ID
      dstVertex addAssociatedEdge edge // do the same for the destination node
      if (!present)
        edge killList dstVertex.removeList
    }

    val srcVertex = vertexAdd(routerID,msgTime, srcId)                          // create or revive the source ID
    srcVertex addAssociatedEdge edge // add the edge to the associated edges of the source node

    if (present) {
      edge revive msgTime
      edge updateLatestRouter routerID
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAdd(routerID,msgTime, srcId, dstId, null),false) // inform the partition dealing with the destination node*/
    } else {
      val deaths = srcVertex.removeList
      edge killList deaths
      if (!local)
        mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeAddNew(routerID,msgTime, srcId, dstId, null, deaths), false)
    }
    GraphRepoProxy.addEdge(edge.getId)
    if (properties != null)
      properties.foreach(prop => edge.updateProp(prop._1, new Property(msgTime, prop._1, prop._2))) // add all passed properties onto the edge

  }

  def updateEdgeProperties(msgTime : Long, edgeId : Long, key : String, value : String) : Unit = {
    edges.get(edgeId) match {
      case Some(e) => {
        e + (msgTime, key, value)
      }
      case None =>
    }

    // If edge is split reroute the update to the remote position
    if (Utils.getPartition(Utils.getIndexLO(edgeId), managerCount) != managerID) {
      mediator ! DistributedPubSubMediator.Send(getManager(Utils.getIndexLO(edgeId), managerCount), EdgeUpdateProperty(msgTime, edgeId, key, value), false)
    }
  }

  def remoteEdgeAdd(routerID : Int, msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String] = null):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge already exists so just updating")
    val dstVertex = vertexAdd(routerID,msgTime,dstId) //create or revive the destination node
    val edge = edges(getEdgeIndex(srcId, dstId))
    edge updateLatestRouter routerID
    dstVertex addAssociatedEdge edge //again I think this can be removed
    edge revive msgTime //revive  the edge
    if (properties != null)
      properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
  }

  def remoteEdgeAddNew(routerID : Int, msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge did not previously exist so sending back deaths")
    val dstVertex = vertexAdd(routerID,msgTime,dstId) //create or revive the destination node
    val edge = new RemoteEdge(routerID, msgTime, srcId, dstId, initialValue = true, addOnlyEdge,RemotePos.Source,getPartition(srcId, managerCount))
    dstVertex addAssociatedEdge edge //add the edge to the associated edges of the destination node
    edges put(getEdgeIndex(srcId,dstId), edge) //create the new edge
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }

  def edgeRemoval(routerID : Int, msgTime:Long, srcId:Int, dstId:Int):Unit={
    val local       = checkDst(dstId, managerCount, managerID)
    var present     = false
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)
    if (local)
      edge = new Edge(routerID, msgTime, srcId, dstId, initialValue = false, addOnlyEdge)
    else
      edge = new RemoteEdge(routerID, msgTime,srcId, dstId, initialValue = false, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.get(index) match {
      case Some(e) => {
        edge = e
        if (windowing) {
          if (!(edge latestRouterCheck routerID)) { //if we are windowing we must check the latest Router for the vertex
            return //if its not from the same router we ignore and return the function here
          }
        }
        present = true
      }
      case None =>
        edges.put(index, edge)
    }

    if (printing) println(s"Received an edge Add for $srcId --> $dstId (local: $local)")
    var dstVertex : Vertex = null
    var srcVertex : Vertex = null


    if (local && srcId != dstId) {
      dstVertex = getVertexAndWipe(routerID,dstId, msgTime)
      dstVertex addAssociatedEdge edge // do the same for the destination node
      if (!present)
        edge killList dstVertex.removeList
    }
    srcVertex = getVertexAndWipe(routerID,srcId, msgTime)
    srcVertex addAssociatedEdge edge // add the edge to the associated edges of the source node

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

  def remoteEdgeRemoval(routerID : Int, msgTime:Long,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge already exists so just updating")
    val dstVertex = getVertexAndWipe(routerID,dstId, msgTime)
    edges.get(getEdgeIndex(srcId, dstId)) match {
      case Some(e) => {
        e kill msgTime
        dstVertex addAssociatedEdge e
      }
      case None    => println("Didn't exist") //possibly need to fix when adding the priority box
    }
  }

  def remoteEdgeRemovalNew(routerID : Int,msgTime:Long,srcId:Int,dstId:Int,srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge did not previously exist so sending back deaths ")
    val dstVertex = getVertexAndWipe(routerID, dstId, msgTime)
    val edge = new RemoteEdge(routerID,msgTime,srcId, dstId, initialValue = false, addOnlyEdge, RemotePos.Source, getPartition(srcId, managerCount))
    dstVertex addAssociatedEdge edge  //add the edge to the destination nodes associated list
    edges put(getEdgeIndex(srcId,dstId), edge) // otherwise create and initialise as false

    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }

  def returnEdgeRemoval(routerID:Int,msgTime:Long,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal (return) for $srcId --> $dstId from ${getManager(dstId, managerCount )}. Edge already exists so just updating")
    val srcVertex = getVertexAndWipe(routerID,srcId, msgTime)
    val edge = edges(getEdgeIndex(srcId, dstId))

    srcVertex addAssociatedEdge edge //add the edge to the destination nodes associated list
    edge kill msgTime                  // if the edge already exists, kill it
  }

  def remoteReturnDeaths(msgTime:Long,srcId:Int,dstId:Int,dstDeaths:mutable.TreeMap[Long, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    edges(getEdgeIndex(srcId,dstId)) killList dstDeaths
  }


//  def retrieveEdge(id:Long):Edge = {
//    val srcId = Utils.getIndexHI(id)
//    val dstId = Utils.getIndexLO(id)
//    val savedEdge = MongoFactory.retrieveEdge(id)
//
//    val history = savedEdge.history
//    val head = history.head
//    if (checkDst(dstId, managerCount, managerID)) {
//      val edge = new Edge(-1, head.time, srcId, dstId, initialValue = head.value, addOnlyEdge)
//      edge.addHistory(history.tail)
//      savedEdge.properties match {
//        case Some(properties) => edge.addProperties(properties)
//        case None =>
//      }
//      edge
//    }
//    else {
//      val edge = new RemoteEdge(-1, head.time, srcId, dstId, initialValue = head.value, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))
//      edge.addHistory(history.tail)
//      savedEdge.properties match {
//        case Some(properties) => edge.addProperties(properties)
//        case None =>
//      }
//      edge
//    }
//  }

//  def retrieveVertex(id:Int):Vertex = {
//    val savedVertex = MongoFactory.retrieveVertex(id)
//    val history = savedVertex.history
//    val head = history.head
//    println(savedVertex)
//    val vertex = new Vertex(-1,head.time,id,head.value,addOnlyVertex)
//    vertex.addHistory(history.tail)
//    savedVertex.properties match {
//      case Some(properties) => vertex.addProperties(properties)
//      case None => {}
//    }
//    savedVertex.associatedEdges match {
//      case Some(associatedEdges) => {
//        for(edgeID <- associatedEdges){
//          println(edgeID)
//          vertex.addAssociatedEdge(retrieveEdge(edgeID))
//        }
//      }
//      case None => {}
//    }
//
//    vertex
//  }

  def compareMemoryToSaved() ={
    vertices.foreach(pair=>{
      val vertex = pair._2
      //vertex.compareHistory()
    })
    edges.foreach(pair=>{
      val edge = pair._2
      // edge.compareHistory()
    })
  }



}
