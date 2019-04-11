package com.raphtory.core.storage

import java.util.NoSuchElementException
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSubMediator

import scala.collection.concurrent.TrieMap
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graphentities.{Edge, Property, RemoteEdge, RemotePos, Vertex}
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.raphtory.core.utils.Utils
import com.raphtory.core.utils.exceptions.{EntityRemovedAtTimeException, PushedOutOfGraphException, StillWithinLiveGraphException}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParSet
import scala.collection.parallel.mutable.ParTrieMap

/**
  * Singleton representing the Storage for the entities
  */
//TODO add capacity function based on memory used and number of updates processed/stored in memory
//TODO What happens when an edge which has been archived gets readded
//TODO: does the routerID effect vertices which are wiped
//TODO: Should vertices be aware of fully archived edges
object EntityStorage {
  import com.raphtory.core.utils.Utils.{checkDst, getEdgeIndex, getPartition, getManager,checkWorker}

  var messageCount          = new AtomicInteger(0)        // number of messages processed since last report to the benchmarker
  var secondaryMessageCount = new AtomicInteger(0)

  val children = 10

  /**
    * Map of vertices contained in the partition
    */
  val vertices        = ParTrieMap[Int, Vertex]()
  val deletedVertices = ParSet[Int]()
  val vertexKeys      = ParTrieMap[Int,ParSet[Int]]()
  for(i <- 0 until children){
    val temp = ParSet[Int]()
    vertexKeys put (i, temp)
  }
  /**
    * Map of edges contained in the partition
    */
  val edges        = ParTrieMap[Long, Edge]()  // Map of Edges contained in the partition
  val edgeKeys     = ParTrieMap[Int,ParSet[Long]]()
  for(i <- 0 until children){
    val temp = ParSet[Long]()
    edgeKeys put (i,temp)
  }


  var printing     : Boolean = true
  var managerCount : Int     = 1
  var managerID    : Int     = 0
  var mediator     : ActorRef= null
  var addOnlyVertex    : Boolean =  System.getenv().getOrDefault("ADD_ONLY_VERTEX", "false").trim.toBoolean
  var addOnlyEdge      : Boolean =  System.getenv().getOrDefault("ADD_ONLY_EDGE", "false").trim.toBoolean
  var windowing        : Boolean =  System.getenv().getOrDefault("WINDOWING", "false").trim.toBoolean

  //stuff for compression and archiving
  var oldestTime:Long = Long.MaxValue
  var newestTime:Long = Long.MinValue
  var lastCompressedAt:Long = 0

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

  def setManagerCount(count : Int) = {
    this.managerCount = count
  }

  def newVertexKey(workerID:Int,id:Int):Unit = {
    vertexKeys(workerID) += id
  } //generate a random number based on the id (as ID's have already been modulated to reach a PM and therefore will probably end in the same number

  def newEdgeKey(workerID:Int,id:Long):Unit = {
    edgeKeys(workerID) += id
  }

  /**
    * Vertices Methods
    */

  def vertexAdd(routerID:Int,workerID:Int,msgTime : Long, srcId : Int, properties : Map[String,String] = null) : Vertex = { //Vertex add handler function
    var value : Vertex = null
    vertices.get(srcId) match { //check if the vertex exists
      case Some(v) => { //if it does
        v revive msgTime //add the history point
        v updateLatestRouter routerID // record the latest router for windowing
        value = v
      }
      case None => { //if it does not exist
        value = new Vertex(routerID,msgTime, srcId, initialValue = true, addOnlyVertex) //create a new vertex
        vertices put(srcId, value) //put it in the map
        newVertexKey(workerID,srcId) // and record its ID
      }
    }
    if (properties != null) { //if the add come with some properties
      properties.foreach(prop => value.updateProp(prop._1, new Property(msgTime, prop._1, prop._2))) // add all passed properties into the vertex
    }
    value //return the vertex
  }

  def vertexWorkerRequest(routerID:Int,workerID:Int,msgTime:Long,dstID:Int,srcForEdge:Int,present:Boolean) ={
    //if the worker creating an edge does not deal with
    val dstVertex = vertexAdd(routerID,workerID,msgTime, dstID) // do the same for the destination ID
    dstVertex addIncomingEdge(srcForEdge) // do the same for the destination node
    if (!present)
      edges.get(getEdgeIndex(srcForEdge, dstID)) match {
        case Some(edge) => edge killList dstVertex.removeList //add the dst removes into the edge
      }
  }
  def vertexWipeWorkerRequest(routerID:Int,workerID:Int,msgTime:Long,dstID:Int,srcForEdge:Int,present:Boolean) ={
    //if the worker creating an edge does not deal with
    val dstVertex = getVertexAndWipe(routerID,workerID,dstID, msgTime) // do the same for the destination ID
    dstVertex addIncomingEdge(srcForEdge) // do the same for the destination node
    if (!present)
      edges.get(getEdgeIndex(srcForEdge, dstID)) match {
        case Some(edge) => edge killList dstVertex.removeList //add the dst removes into the edge
      }
  }

  def getVertexAndWipe(routerID : Int, workerID:Int, id : Int, msgTime : Long) : Vertex = {
    vertices.get(id) match {
      case Some(value) => value
      case None => {
        val x  = new Vertex(routerID, msgTime,id,initialValue = true, addOnlyVertex)
        vertices put(id, x)
        x wipe()
        newVertexKey(workerID,id) //and record the new key
        x
      }
    }
  }

  def vertexRemoval(workerID:Int,routerID:Int,msgTime:Long,srcId:Int) : Unit = {
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
        vertex = new Vertex(routerID,msgTime, srcId, initialValue = false, addOnly = addOnlyVertex) //create a placeholder
        vertices put(srcId, vertex) //add it to the map
        newVertexKey(workerID,srcId) //and record the new key
      }
    }

    vertex.incomingIDs.foreach(eID =>{ //incoming edges are not handled by this worker as the source vertex is in charge
      edges.get(Utils.getEdgeIndex(eID,srcId)) match {
        case Some(edge) => {
          if(edge.isInstanceOf[RemoteEdge]) {
            val remoteEdge = edge.asInstanceOf[RemoteEdge]
            if (remoteEdge.remotePos == RemotePos.Source) {
              edge kill  msgTime
              mediator ! DistributedPubSubMediator.Send(getManager(remoteEdge.getSrcId, managerCount), ReturnEdgeRemoval(routerID,msgTime, remoteEdge.srcId, remoteEdge.dstId), false) //inform the other partition to do the same
            } //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
            else {
              println("incoming"+" "+vertex.getId+ " "+edge +" "+ managerID +" " + workerID + "")
              //edge kill msgTime //kill the edge
              //
            } //This is the case if the remote vertex is the source of the edge. In this case we handle it with the specialised function below

          }
          else{ //if it is a local edge
            if(edge.getWorkerID == workerID) { //opperated by the same worker, therefore we can perform an action
              edge kill msgTime
            }
            else{ //we must inform the other local worker to handle this
              mediator ! DistributedPubSubMediator.Send(getManager(edge.getSrcId, managerCount), EdgeRemoveForOtherWorker(routerID,msgTime,edge.getSrcId,edge.getDstId),false)
            }
          }
        }
        case None => { println("none 2") /*edge has been archived */}
      }
    })
    vertex.outgoingIDs.foreach(id =>{
      edges.get(Utils.getEdgeIndex(srcId,id)) match {
        case Some(edge) => {
          edge kill msgTime//outgoing edge always opperated by the same worker, therefore we can perform an action
          if(edge.isInstanceOf[RemoteEdge]){
            val remoteEdge = edge.asInstanceOf[RemoteEdge]
            if (remoteEdge.remotePos == RemotePos.Destination) {
              mediator ! DistributedPubSubMediator.Send(getManager(edge.getDstId, managerCount), RemoteEdgeRemoval(routerID,msgTime, remoteEdge.srcId, remoteEdge.dstId), false)
            } //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
            else {
              println("outgoing"+" "+vertex.getId+ " "+edge +" "+ managerID +" " + workerID + "")
              //mediator ! DistributedPubSubMediator.Send(getManager(remoteEdge.remotePartitionID, managerCount), RemoteEdgeRemoval(routerID,msgTime, remoteEdge.srcId, remoteEdge.dstId), false)
            } //This is the case if the remote vertex is the source of the edge. In this case we handle it with the specialised function below
          }
        }
        case None => { println("none 1")/*edge has been archived */}
      }
    })
  }

  def returnEdgeRemoval(routerID:Int,workerID:Int,msgTime:Long,srcId:Int,dstId:Int):Unit={ //for the source getting an update abou
    val srcVertex = getVertexAndWipe(routerID,workerID,srcId, msgTime)
    val edge = edges(getEdgeIndex(srcId, dstId))
    edge kill msgTime
  }


  def edgeRemovalFromOtherWorker(routerID:Int,msgTime:Long,srcID:Int,dstID:Int) = {
    edges.get(Utils.getEdgeIndex(srcID,dstID)) match {
      case Some(edge) => {
        edge kill msgTime
      }
    }
  }

//
  /**
    * Edges Methods
    */
  def edgeAdd(routerID:Int,workerID:Int,msgTime : Long, srcId : Int, dstId : Int, properties : Map[String, String] = null) = {
    val local       = checkDst(dstId, managerCount, managerID) //is the dst on this machine
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker
    var present     = false //if the vertex is new or not -- decides what update is sent when remote and if to add the source/destination removals
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)

    edges.get(index) match {
      case Some(e) => { //retrieve the edge if it exists
        edge = e
        present = true
      }
      case None => //if it does not
        if (local)
          edge = new Edge(routerID,workerID,msgTime, srcId, dstId, initialValue = true, addOnlyEdge) //create the new edge, local or remote
        else
          edge = new RemoteEdge(routerID,workerID,msgTime, srcId, dstId, initialValue = true, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))
        edges.put(index, edge) //add this edge to the map
        newEdgeKey(workerID,index) //and record its ID for the compressor
    }

    val srcVertex = vertexAdd(routerID,workerID,msgTime, srcId) // create or revive the source ID
    srcVertex addOutgoingEdge(dstId) // add the edge to the associated edges of the source node

    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = vertexAdd(routerID,workerID,msgTime, dstId) // do the same for the destination ID
        dstVertex addIncomingEdge(srcId) // do the same for the destination node
        if (!present)
          edge killList dstVertex.removeList //add the dst removes into the edge
      }
      else{ // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstAddForOtherWorker(routerID,msgTime,dstId,srcId,present),true)
      }
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

    if (properties != null)
      properties.foreach(prop => edge.updateProp(prop._1, new Property(msgTime, prop._1, prop._2))) // add all passed properties onto the edge

  }

  def remoteEdgeAddNew(routerID : Int, workerID:Int, msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = vertexAdd(routerID,workerID,msgTime,dstId) //create or revive the destination node
    val edge = new RemoteEdge(routerID, workerID, msgTime, srcId, dstId, initialValue = true, addOnlyEdge,RemotePos.Source,getPartition(srcId, managerCount))
    dstVertex addIncomingEdge(srcId) //add the edge to the associated edges of the destination node
    val index = getEdgeIndex(srcId,dstId)
    edges put(index, edge) //create the new edge
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    newEdgeKey(workerID,index)
    if (properties != null)
      properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }

  def remoteEdgeAdd(routerID : Int,workerID:Int, msgTime:Long,srcId:Int,dstId:Int,properties:Map[String,String] = null):Unit={
    val dstVertex = vertexAdd(routerID,workerID,msgTime,dstId) // revive the destination node
    val edge = edges(getEdgeIndex(srcId, dstId))
    edge updateLatestRouter routerID
    dstVertex addIncomingEdge(srcId) //again I think this can be removed
    edge revive msgTime //revive the edge
    if (properties != null)
      properties.foreach(prop => edge + (msgTime,prop._1,prop._2)) // add all passed properties onto the list
  }



  def edgeRemoval(routerID : Int, workerID:Int,msgTime:Long, srcId:Int, dstId:Int):Unit={
    val local       = checkDst(dstId, managerCount, managerID)
    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker

    var present     = false
    var edge : Edge = null
    val index : Long= getEdgeIndex(srcId, dstId)
    edges.get(index) match {
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
          edge = new Edge(routerID,workerID, msgTime, srcId, dstId, initialValue = false, addOnlyEdge)
        else
          edge = new RemoteEdge(routerID, workerID, msgTime,srcId, dstId, initialValue = false, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))
        edges.put(index, edge)
        newEdgeKey(workerID,index)
      }
    }

    var srcVertex : Vertex  = getVertexAndWipe(routerID,workerID,srcId, msgTime)
    srcVertex addOutgoingEdge(dstId) // add the edge to the associated edges of the source node

    if (local && srcId != dstId) {
      if(sameWorker){ //if the dst is handled by the same worker
        val dstVertex = getVertexAndWipe(routerID,workerID,dstId, msgTime) // do the same for the destination ID
        dstVertex addIncomingEdge(srcId) // do the same for the destination node
        if (!present)
          edge killList dstVertex.removeList //add the dst removes into the edge
      }
      else{ // if it is a different worker, ask that other worker to complete the dst part of the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstWipeForOtherWorker(routerID,msgTime,dstId,srcId,present),true)
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

  def remoteEdgeRemoval(routerID : Int,workerID:Int,msgTime:Long,srcId:Int,dstId:Int):Unit={
    val dstVertex = getVertexAndWipe(routerID,workerID,dstId, msgTime)
    edges.get(getEdgeIndex(srcId, dstId)) match {
      case Some(e) => {
        e kill msgTime
      }
      case None    => println(s"Worker ID $workerID Manager ID $managerID")
    }
  }


  def remoteEdgeRemovalNew(routerID : Int,workerID:Int,msgTime:Long,srcId:Int,dstId:Int,srcDeaths:mutable.TreeMap[Long, Boolean]):Unit={
    val dstVertex = getVertexAndWipe(routerID, workerID,dstId, msgTime)
    val edge = new RemoteEdge(routerID,workerID,msgTime,srcId, dstId, initialValue = false, addOnlyEdge, RemotePos.Source, getPartition(srcId, managerCount))
    dstVertex addIncomingEdge(srcId)  //add the edge to the destination nodes associated list
    val index = getEdgeIndex(srcId,dstId)
    edges put(index, edge) // otherwise create and initialise as false
    newEdgeKey(workerID,index)
    val deaths = dstVertex.removeList //get the destination node deaths
    edge killList srcDeaths //pass source node death lists to the edge
    edge killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount),RemoteReturnDeaths(msgTime,srcId,dstId,deaths),false)
  }



  def remoteReturnDeaths(msgTime:Long,srcId:Int,dstId:Int,dstDeaths:mutable.TreeMap[Long, Boolean]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId, managerCount)}")
    edges(getEdgeIndex(srcId,dstId)) killList dstDeaths
  }

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

  def createSnapshot(time:Long):ParTrieMap[Int, Vertex] = {
    val snapshot:ParTrieMap[Int, Vertex] = ParTrieMap[Int, Vertex]()
    var count = 0
    for ((k,v) <- vertices) {
      try {
        val vertex =v.viewAt(time)
        snapshot.put(k,vertex )
        count +=1
      }
      catch {
        case e:EntityRemovedAtTimeException => //println(e)
        case e:PushedOutOfGraphException => println(e)
        case e:StillWithinLiveGraphException => println(e)
      }
    }
    println(s"$count entities in snapshot")
    snapshot

  }


}

//def updateEdgeProperties(msgTime : Long, edgeId : Long, key : String, value : String) : Unit = {
//  edges.get(edgeId) match {
//  case Some(e) => {
//  e + (msgTime, key, value)
//}
//  case None =>
//}
//
//  // If edge is split reroute the update to the remote position
//  if (Utils.getPartition(Utils.getIndexLO(edgeId), managerCount) != managerID) {
//  mediator ! DistributedPubSubMediator.Send(getManager(Utils.getIndexLO(edgeId), managerCount), EdgeUpdateProperty(msgTime, edgeId, key, value), false)
//}
//}

//
//try {
//  vertexKeys(Utils.getWorker(id,managerCount)) += id
//}
//  catch {
//  case e:ArrayIndexOutOfBoundsException => {
//  println(s"Caught array error with Vertex Keys, for id $id accessing Set ${new scala.util.Random(id).nextInt(1000) % children}, rerunning")
//  newVertexKey(id)
//}
//}

//def incomingEdgeRemovalAfterArchiving(routerID:Int, workerID:Int, msgTime:Long, srcId:Int, dstId:Int) : Unit = { //turned off for now
//   val local       = checkDst(srcId, managerCount, managerID)
//   val sameWorker  = checkWorker(srcId,managerCount,workerID) // is the dst handled by the same worker
//   var present     = false
//   var edge : Edge = null
//   if (local) { //if it is local
//     if (sameWorker) { //and the same worker
//       edge = new Edge(routerID, workerID, msgTime, srcId, dstId, initialValue = false, addOnlyEdge)
//       if(srcId != dstId){
//           val dstVertex = getVertexAndWipe(routerID,workerID,dstId, msgTime) // do the same for the destination ID
//           dstVertex addAssociatedEdge (srcId, false) // do the same for the destination node
//           edge killList dstVertex.removeList //add the dst removes into the edge
//       }
//     }
//     else { //if it is a different worker on the same machine we must pass it to them to handle
//       mediator ! DistributedPubSubMediator.Send(getManager(srcId, managerCount), EdgeRemovalAfterArchiving(routerID, msgTime, srcId, srcId), false)
//     }
//   }
//   else { //if it is not local then we also handle it
//   }
//
// }
//
//  def outGoingEdgeRemovalAfterArchiving(routerID:Int, workerID:Int, msgTime:Long, srcId:Int, dstId:Int) : Unit = {  //turned off for now
//    val local       = checkDst(dstId, managerCount, managerID)
//    val sameWorker  = checkWorker(dstId,managerCount,workerID) // is the dst handled by the same worker
//
//    var present     = false
//    var edge : Edge = null
//    val index : Long= getEdgeIndex(srcId, dstId)
//    if (local) {
//      edge = new Edge(routerID, workerID, msgTime, srcId, dstId, initialValue = false, addOnlyEdge)
//      if(srcId != dstId){
//        if(sameWorker){ //if the dst is handled by the same worker
//          val dstVertex = getVertexAndWipe(routerID,workerID,dstId, msgTime) // do the same for the destination ID
//          dstVertex addAssociatedEdge (srcId, false) // do the same for the destination node
//          edge killList dstVertex.removeList //add the dst removes into the edge
//        }
//        else{ // if it is a different worker, ask that other worker to complete the dst part of the edge
//          mediator ! DistributedPubSubMediator.Send(getManager(dstId,managerCount),DstWipeForOtherWorker(routerID,msgTime,dstId,srcId,present),true)
//        }
//      }
//    }
//    else {
//      edge = new RemoteEdge(routerID, workerID, msgTime, srcId, dstId, initialValue = false, addOnlyEdge, RemotePos.Destination, getPartition(dstId, managerCount))
//      mediator ! DistributedPubSubMediator.Send(getManager(dstId, managerCount), RemoteEdgeRemoval(routerID,msgTime,srcId,dstId),false) // inform the partition dealing with the destination node
//    }
//    edges.put(index, edge)
//  }
//
