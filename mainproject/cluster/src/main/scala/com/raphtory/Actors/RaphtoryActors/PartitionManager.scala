package com.raphtory.Actors.RaphtoryActors

import java.io._

import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.caseclass._
import com.raphtory.GraphEntities._
import akka.event.Logging
import com.raphtory.caseclass._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */

//need to work out why the return death is happening multiple times
class PartitionManager(id : Int, test : Boolean, managerCount : Int) extends RaphtoryActor{
  val childID  = id                     //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]()      // Map of Vertices contained in the partition
  var edges    = Map[(Int,Int),Edge]()  // Map of Edges contained in the partition

  val loggger  = Logging(context.system, this)
  val printing = false                  //should the handled messages be printed to terminal
  val logging  = false                  // should the state of the vertex/edge map be output to file

  var messageCount          = 0         // number of messages processed since last report to the benchmarker
  var secondaryMessageCount = 0
  var messageBlockID        = 0         // id of current message block to syncronise times across partition managers
  val secondaryCounting     = false     // count all messages or just main incoming ones

  val mediator              = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart() {             // set up partition to report how many messages it has processed in the last X seconds
    context.system.scheduler.schedule(Duration(7, SECONDS),
      Duration(2, SECONDS), self, "tick")
    context.system.scheduler.schedule(Duration(13, SECONDS),
      Duration(5, MINUTES), self, "profile")
    context.system.scheduler.schedule(Duration(10, SECONDS),
      Duration(10, SECONDS), self, "keep_alive")
  }

  def reportIntake() : Unit = {
    if(printing)
      println(messageCount)
    // TODO Put the partition manager Id
    // Kamon monitoring
    kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount").set(messageCount)
    kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount").set(secondaryMessageCount)

    // Heap benchmarking
    //profile()

    // Set counters
    messageCount          = 0
    secondaryMessageCount = 0
    messageBlockID        = messageBlockID + 1
  }

  def keepAlive() = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", PartitionUp(childID), false)

  def vHandle(srcID : Int) : Unit = {
    messageCount = messageCount + 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "messageCounter").increment()
    log(srcID)
  }

  def vHandleSecondary(srcID : Int) : Unit = {
    secondaryMessageCount = secondaryMessageCount + 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCounter").increment()
    log(srcID)
  }
  def eHandle(srcID : Int, dstID : Int) : Unit = {
    messageCount = messageCount + 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "messageCounter").increment()
    log(srcID, dstID)
    log(srcID)
    log(dstID)
  }

  def eHandleSecondary(srcID : Int, dstID : Int) : Unit = {
    secondaryMessageCount = secondaryMessageCount + 1
    kCounter.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCounter").increment()
    log(srcID,dstID)
    log(srcID)
    log(dstID)
  }

  override def receive : Receive = {

    case "tick" => reportIntake()
    case "profile" => profile()
    case "keep_alive" => keepAlive()

    case LiveAnalysis(name,analyser) => mediator ! DistributedPubSubMediator.Send(name, Results(analyser.analyse(vertices,edges)), false)

    case VertexAdd(msgId,srcId) => vertexAdd(msgId,srcId); vHandle(srcId)
    case VertexAddWithProperties(msgId,srcId,properties) => vertexAddWithProperties(msgId,srcId,properties); vHandle(srcId);
    case VertexUpdateProperties(msgId,srcId,properties) => vertexUpdateProperties(msgId,srcId,properties); vHandle(srcId);
    case VertexRemoval(msgId,srcId) => vertexRemoval(msgId,srcId); vHandle(srcId);

    case EdgeAdd(msgId,srcId,dstId) => edgeAdd(msgId,srcId,dstId); eHandle(srcId,dstId)
    case RemoteEdgeAdd(msgId,srcId,dstId) => remoteEdgeAdd(msgId,srcId,dstId); eHandleSecondary(srcId,dstId)
    case RemoteEdgeAddNew(msgId,srcId,dstId,deaths) =>  remoteEdgeAddNew(msgId,srcId,dstId,deaths); eHandleSecondary(srcId,dstId)

    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAddWithProperties(msgId,srcId,dstId,properties); eHandle(srcId,dstId)
    case RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) => remoteEdgeAddWithProperties(msgId,srcId,dstId,properties); eHandleSecondary(srcId,dstId)
    case RemoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths) => remoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths); eHandleSecondary(srcId,dstId)

    case EdgeUpdateProperties(msgId,srcId,dstId,properties) => edgeUpdateWithProperties(msgId,srcId,dstId,properties); eHandle(srcId,dstId)
    case RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) => remoteEdgeUpdateWithProperties(msgId,srcId,dstId,properties); eHandleSecondary(srcId,dstId)

    case EdgeRemoval(msgId,srcId,dstId) => edgeRemoval(msgId,srcId,dstId); eHandle(srcId,dstId)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId); eHandleSecondary(srcId,dstId)
    case RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths) => remoteEdgeRemovalNew(msgId,srcId,dstId,deaths); eHandleSecondary(srcId,dstId)

    case RemoteReturnDeaths(msgId,srcId,dstId,deaths) => remoteReturnDeaths(msgId,srcId,dstId,deaths); eHandleSecondary(srcId,dstId)
    case ReturnEdgeRemoval(msgId,srcId,dstId) => returnEdgeRemoval(msgId,srcId,dstId);  eHandleSecondary(srcId,dstId)

  }

  def vertexAdd(msgId : Int, srcId : Int) : Unit = { //Vertex add handler function
    if (!(vertices contains srcId))   // if the vertex doesn't already exist, create it and add it to the vertex map
      vertices = vertices updated(srcId, new Vertex(msgId, srcId, true))
    else                              // if it does exist, store the add in the vertex state
      vertices(srcId) revive msgId

    if (printing)
      println(s"Received a Vertex Add for $srcId")
  }

  def vertexAddWithProperties(msgId : Int,srcId : Int, properties : Map[String,String]) : Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2)) //add all properties
    if(printing) println(s"Received a Vertex Add with properties for $srcId")
  }

  def edgeAdd(msgId : Int, srcId : Int, dstId : Int) : Unit = {
    if (printing) println(s"Received an edge Add for $srcId --> $dstId")
    if (checkDst(dstId)) {                             // local edge
      vertexAdd(msgId, srcId)                          // create or revive the source ID
      if (srcId != dstId)
        vertexAdd(msgId, dstId)                        // do the same for the destination ID

      vertices(srcId) addAssociatedEdge (srcId, dstId) // add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId, dstId) // do the same for the destination node

      if (edges contains (srcId, dstId))               // if the edge already exists revive
        edges(srcId, dstId) revive msgId
      else {                                           //if the edge is yet to exist
        edges = edges updated((srcId, dstId), new Edge(msgId, true, srcId, dstId))
        edges(srcId, dstId) killList vertices(srcId).removeList.map(kill => kill._1) // get the remove list from source node and give to the Edge
        if (srcId != dstId)
          edges(srcId, dstId) killList vertices(dstId).removeList.map(kill => kill._1) // get the remove list from destination node and give to the Edge
      }
    }
    else {                                            // remote edge
      if(printing)
        println(s"    Is a remote edge: Sending to ${getManager(dstId)}")
      vertexAdd(msgId,srcId)                          // create or revive the source ID
      vertices(srcId) addAssociatedEdge(srcId,dstId)  // add the edge to the associated edges of the source node

      if (edges contains (srcId,dstId)) {             // if the edge already exists
        edges(srcId,dstId) revive msgId               // revive
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),RemoteEdgeAdd(msgId,srcId,dstId),false) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),
          new RemoteEdge(msgId, true, srcId, dstId, RemotePos.Destination, getPartition(dstId)))  // create the remote edge
        val deaths = vertices(srcId).removeList.map(kill => kill._1)                              // retrieve all the deaths present within the source node
        edges(srcId,dstId) killList deaths                                                        // and add them to the new edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),
          RemoteEdgeAddNew(msgId, srcId, dstId, deaths), false)                                       // inform the partition dealing with the destination node (also send the deaths so they can be added on the mirror side)
      }
    }
  }

  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Add for $srcId --> $dstId. Edge already exists so just updating")
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //don't think this is needed, may be able to remove
    edges((srcId,dstId)) revive msgId //revive the edge
  }

  def remoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,srcDeaths:List[Int]):Unit={
    if(printing) println(s"Received Remote Edge Add for $srcId --> $dstId. Edge did not previously exist so adding deaths from src and dst and sending dst deaths back to src partition")
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //create the new edge
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false) //return to the src manager the deaths present within the dst to finalise sync
  }

  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(printing) println(s"Received an edge Add with properties for $srcId --> $dstId")
    if(checkDst(dstId)) { //local edge
      vertexAdd(msgId,srcId) //create or revive the source ID
      if(srcId!=dstId)vertexAdd(msgId,dstId) //do the same for the destination ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node

      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else {
        edges = edges updated((srcId,dstId),new Edge(msgId,true,srcId,dstId))
        edges(srcId,dstId) killList vertices(srcId).removeList.map(kill => kill._1) //get the remove list from source node and give to the Edge
        if(srcId!=dstId)edges(srcId,dstId) killList vertices(dstId).removeList.map(kill => kill._1) //get the remove list from destination node and give to the Edge
      } //if the edge is yet to exist
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
    }

    else{ //remote edge
      if(printing) println(s"    Is a remote edge: Sending to ${getManager(dstId)}")
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node

      if(edges contains (srcId,dstId)) { //if the edge already exists
        edges(srcId,dstId) revive msgId // revive
        properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties),false) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Destination,getPartition(dstId))) //create the remote edge
        val deaths = vertices(srcId).removeList.map(kill => kill._1) //get the source node deaths
        edges(srcId,dstId) killList deaths //pass to teh edge
        properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),RemoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths),false) // inform the partition dealing with the destination node
      }
    }
  }

  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId)}. Edge already exists so just updating")
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //again I think this can be removed
    edges((srcId,dstId)) revive msgId //revive  the edge
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }

  def remoteEdgeAddWithPropertiesNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:List[Int]):Unit={
    if(printing) println(s"Received Remote Edge Add with properties for $srcId --> $dstId from ${getManager(srcId)}. Edge did not previously exist so sending back deaths")
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //create the new edge
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    mediator ! DistributedPubSubMediator.Send(getManager(srcId),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def edgeRemoval(msgId:Int, srcId:Int, dstId:Int):Unit={
    if(printing) println(s"Received Edge removal for $srcId --> $dstId")
    if(checkDst(dstId)) { //local edge
      if(!(vertices contains srcId)){ //if src vertex does not exist, create it and wipe the history so that it may contain the associated Edge list
        vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
        vertices(srcId) wipe()
      }
      if(!(vertices contains dstId)){ //do the same for the destination node
        vertices = vertices updated(dstId,new Vertex(msgId,dstId,true))
        vertices(dstId) wipe()
      }
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node

      if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
      else {
        edges = edges updated((srcId, dstId), new Edge(msgId, false, srcId, dstId)) // otherwise create and initialise as false
        edges(srcId, dstId) killList vertices(srcId).removeList.map(kill => kill._1) //get the remove list from source node and give to the Edge
        if(srcId!=dstId)edges(srcId, dstId) killList vertices(dstId).removeList.map(kill => kill._1) //get the remove list from destination node and give to the Edge (if it isn't a loop edge)
      }
    }
    else { // remote edge
      if(printing) println(s"   Is Remote Edge, sending remove to ${getManager(dstId)}")
      if(!(vertices contains srcId)){ //if src vertex does not exist, create it and wipe the history so that it may contain the associated Edge list
        vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
        vertices(srcId) wipe()
      }
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node

      if(edges contains (srcId,dstId)) { //if the edge already exists
        edges((srcId,dstId)) kill msgId // kill it
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),RemoteEdgeRemoval(msgId,srcId,dstId),false) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Destination,getPartition(dstId))) // otherwise create and initialise as false
        val deaths = vertices(srcId).removeList.map(kill => kill._1) //get the source node deaths
        edges(srcId,dstId) killList deaths //pass to the edge
        mediator ! DistributedPubSubMediator.Send(getManager(dstId),RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths),false) // inform the partition dealing with the destination node
      }
    }
  }

  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId)}. Edge already exists so just updating")
    if(!(vertices contains dstId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(dstId,new Vertex(msgId,dstId,true))
      vertices(dstId) wipe()
    }
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list
    if(edges contains (srcId,dstId))
      edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
    else
      println("Didn't exist") //possibly need to fix when adding the priority box
  }

  def remoteEdgeRemovalNew(msgId:Int,srcId:Int,dstId:Int,srcDeaths:List[Int]):Unit={
    if(printing) println(s"Received Remote Edge Removal with properties for $srcId --> $dstId from ${getManager(srcId)}. Edge did not previously exist so sending back deaths ")

    if(!(vertices contains dstId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(dstId,new Vertex(msgId,dstId,true))
      vertices(dstId) wipe()
    }
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Source,getPartition(srcId))) // otherwise create and initialise as false
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge

    mediator ! DistributedPubSubMediator.Send(getManager(srcId),RemoteReturnDeaths(msgId,srcId,dstId,deaths),false)
  }

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    if(printing) println(s"Received vertex remove for $srcId, updating + informing all edges")
    if(vertices contains srcId) {
      vertices(srcId) kill msgId
    } //if the vertex already exists then kill it
    else {
      vertices = vertices updated(srcId,new Vertex(msgId,srcId,false))
    } //create a new vertex, but initialise as false
    vertices(srcId).associatedEdges.foreach(eKey  =>{
      edges(eKey) kill msgId //kill the edge
      if(edges(eKey).isInstanceOf[RemoteEdge]){
        if(edges(eKey).asInstanceOf[RemoteEdge].remotePos==RemotePos.Destination) {
          mediator ! DistributedPubSubMediator.Send(getManager(edges(eKey).asInstanceOf[RemoteEdge].remotePartitionID),RemoteEdgeRemoval(msgId, eKey._1, eKey._2),false)
        } //This is if the remote vertex (the one not handled) is the edge destination. In this case we handle with exactly the same function as above
        else{
          mediator ! DistributedPubSubMediator.Send(getManager(edges(eKey).asInstanceOf[RemoteEdge].remotePartitionID),ReturnEdgeRemoval(msgId, eKey._1, eKey._2),false)
        }//This is the case if the remote vertex is the source of the edge. In this case we handle it with the specialised function below
      }
      log(eKey._1,eKey._2)
    })
  }

  def returnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(printing) println(s"Received Remote Edge Removal (return) for $srcId --> $dstId from ${getManager(dstId )}. Edge already exists so just updating")
    if(!(vertices contains srcId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
      vertices(srcId) wipe()
    }
    vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list
    edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
  }

  def remoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,dstDeaths:List[Int]):Unit= {
    if(printing) println(s"Received deaths for $srcId --> $dstId from ${getManager(dstId)}")
    edges(srcId,dstId) killList dstDeaths
  }

  //***************** EDGE HELPERS
  def checkDst(dstID:Int):Boolean = if(dstID%managerCount==childID) true else false //check if destination is also local

  def getPartition(ID:Int):Int = ID%managerCount//get the partition a vertex is stored in

  //*******************PRINT BLOCK
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
  //*******************END PRINT BLOCK

  def edgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= edgeAddWithProperties(msgId,srcId,dstId,properties)

  def vertexUpdateProperties(msgId:Int,srcId:Int,properties:Map[String,String]):Unit = vertexAddWithProperties(msgId,srcId,properties)

  def remoteEdgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= remoteEdgeAddWithProperties(msgId,srcId,dstId,properties)

  def getManager(srcId:Int):String = s"/user/Manager_${srcId % managerCount}"
}
