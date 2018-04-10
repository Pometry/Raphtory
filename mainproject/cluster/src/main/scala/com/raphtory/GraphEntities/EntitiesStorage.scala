package com.raphtory.GraphEntities

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator

import scala.collection.concurrent.TrieMap
import com.raphtory.caseclass._

import scala.collection.mutable

/**
  * Singleton representing the Storage for the entities
  */
object EntitiesStorage {
  import com.raphtory.utils.Utils.{checkDst, getEdgeIndex, getPartition, getManager}
  /**
    * Map of vertices contained in the partition
    */
  val vertices = TrieMap[Int, Vertex]()

   /**
    * Map of edges contained in the partition
    */
  val edges    = TrieMap[Long, Edge]()  // Map of Edges contained in the partition

  var printing     : Boolean = false
  var managerCount : Int     = -1
  var managerID    : Int     = -1
  var mediator     : ActorRef= null
  var addOnly      : Boolean = false

  def apply(printing : Boolean, managerCount : Int, managerID : Int, mediator : ActorRef, addOnly : Boolean) = {
    this.printing     = printing
    this.managerCount = managerCount
    this.managerID    = managerID
    this.mediator     = mediator
    this.addOnly      = addOnly
    this
  }

  /**
    * Vertices Methods
    */
  def vertexAdd(msgId : Int, srcId : Int, properties : Map[String,String] = null) : Vertex = { //Vertex add handler function
    var value : Vertex = new Vertex(msgId, srcId, true, addOnly)
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

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    if (printing) println(s"Received vertex remove for $srcId, updating + informing all edges")
    var vertex : Vertex = null
    vertices.get(srcId) match {
      case Some(v) => {
        vertex = v
        v kill msgId
      }
      case None    => {
        vertex = new Vertex(msgId, srcId, false, addOnly)
        vertices put (srcId, vertex)
      }
    }

    vertex.associatedEdges.foreach(e => {
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

  def getVertexAndWipe(id : Int, msgId : Int) : Vertex = {
    vertices.get(id) match {
      case Some(value) => value
      case None => {
        val x  = new Vertex(msgId,id,true, addOnly)
        vertices put(id, x)
        x wipe()
        x
      }
    }
  }

  /**
    * Edges Methods
    */
  def edgeAdd(msgId : Int, srcId : Int, dstId : Int, properties : Map[String, String] = null) = {
    val local       = checkDst(dstId, managerCount, managerID)
    var present     = false
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)
    if (local)
      edge = new Edge(msgId, true, addOnly, srcId, dstId)
    else
      edge = new RemoteEdge(msgId, true, addOnly, srcId, dstId, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.putIfAbsent(index, edge) match {
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
    val dstVertex = vertexAdd(msgId,dstId) //create or revive the destination node
    val edge = edges(getEdgeIndex(srcId, dstId))
    dstVertex addAssociatedEdge edge //again I think this can be removed
    edge revive msgId //revive  the edge
    if (properties != null)
      properties.foreach(prop => edge + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }

  def remoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:mutable.TreeMap[Int, Boolean]):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId, managerCount)}. Edge did not previously exist so sending back deaths")
    val dstVertex = vertexAdd(msgId,dstId) //create or revive the destination node
    val edge = new RemoteEdge(msgId,true, addOnly, srcId,dstId,RemotePos.Source,getPartition(srcId, managerCount))
    dstVertex addAssociatedEdge edge //add the edge to the associated edges of the destination node
    edges put(getEdgeIndex(srcId,dstId), edge) //create the new edge
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    properties.foreach(prop => edge + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def edgeRemoval(msgId:Int, srcId:Int, dstId:Int):Unit={
    val local       = checkDst(dstId, managerCount, managerID)
    var present     = false
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)
    if (local)
      edge = new Edge(msgId, true, addOnly, srcId, dstId)
    else
      edge = new RemoteEdge(msgId, true, addOnly, srcId, dstId, RemotePos.Destination, getPartition(dstId, managerCount))

    edges.putIfAbsent(index, edge) match {
      case Some(e) => {
        edge = e
        present = true
      }
      case None => // All is set
    }
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
    val dstVertex = getVertexAndWipe(dstId, msgId)
    edges.get(getEdgeIndex(srcId, dstId)) match {
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
    val edge = new RemoteEdge(msgId,false, addOnly, srcId,dstId,RemotePos.Source,getPartition(srcId, managerCount))
    dstVertex addAssociatedEdge edge  //add the edge to the destination nodes associated list
    edges put(getEdgeIndex(srcId,dstId), edge) // otherwise create and initialise as false

    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def returnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal (return) for $srcId --> $dstId from ${getManager(dstId, managerCount )}. Edge already exists so just updating")
    val srcVertex = getVertexAndWipe(srcId, msgId)
    val edge = edges(getEdgeIndex(srcId, dstId))

    srcVertex addAssociatedEdge edge //add the edge to the destination nodes associated list
    edge kill msgId                  // if the edge already exists, kill it
  }

  def remoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,dstDeaths:mutable.TreeMap[Int, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    edges(getEdgeIndex(srcId,dstId)) killList dstDeaths
  }
}
