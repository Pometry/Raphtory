package com.gwz.dockerexp.Actors.RaphtoryActors

import java.io._

import akka.actor.{Actor, ActorRef}
import com.gwz.dockerexp.caseclass._
import com.gwz.dockerexp.GraphEntities._
/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */

//need to work out why the return death is happening multiple times

class GraphPartition(id:Int,test:Boolean) extends Actor {
  val childID = id  //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]() // Map of Vertices contained in the partition
  var edges = Map[(Int,Int),Edge]() // Map of Edges contained in the partition
  var partitionList = Map[Int,ActorRef]()
  val logging = true
  val testPartition = test
 var count = 0
  override def receive: Receive = {

    case PassPartitionList(pl) => partitionList = pl

    case VertexAdd(msgId,srcId) => vertexAdd(msgId,srcId); log(srcId)
    case VertexAddWithProperties(msgId,srcId,properties) => vertexAddWithProperties(msgId,srcId,properties); log(srcId)
    case VertexUpdateProperties(msgId,srcId,properties) => vertexUpdateProperties(msgId,srcId,properties); log(srcId)
    case VertexRemoval(msgId,srcId) => vertexRemoval(msgId,srcId); log(srcId)

    case EdgeAdd(msgId,srcId,dstId) => edgeAdd(msgId,srcId,dstId); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAdd(msgId,srcId,dstId) => remoteEdgeAdd(msgId,srcId,dstId); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAddNew(msgId,srcId,dstId,deaths) =>  remoteEdgeAddNew(msgId,srcId,dstId,deaths); log(msgId,srcId,dstId); log(srcId); log(dstId)

    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAddWithProperties(msgId,srcId,dstId,properties); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) => remoteEdgeAddWithProperties(msgId,srcId,dstId,properties); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths) => remoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths); log(msgId,srcId,dstId); log(srcId); log(dstId)

    case EdgeUpdateProperties(msgId,srcId,dstId,properties) => edgeUpdateWithProperties(msgId,srcId,dstId,properties); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) => remoteEdgeUpdateWithProperties(msgId,srcId,dstId,properties); log(msgId,srcId,dstId); log(srcId); log(dstId)

    case EdgeRemoval(msgId,srcId,dstId) => edgeRemoval(msgId,srcId,dstId); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths) => remoteEdgeRemovalNew(msgId,srcId,dstId,deaths); log(msgId,srcId,dstId); log(srcId); log(dstId)

    case RemoteReturnDeaths(msgId,srcId,dstId,deaths) => remoteReturnDeaths(msgId,srcId,dstId,deaths); log(msgId,srcId,dstId); log(srcId); log(dstId)
    case ReturnEdgeRemoval(msgId,srcId,dstId) => returnEdgeRemoval(msgId,srcId,dstId); log(msgId,srcId,dstId); log(srcId); log(dstId)
  }

  def vertexAdd(msgId:Int,srcId:Int): Unit ={ //Vertex add handler function
    if(!(vertices contains srcId))vertices = vertices updated(srcId,new Vertex(msgId,srcId,true)) //if the vertex doesn't already exist, create it and add it to the vertex map
    else vertices(srcId) revive msgId //if it does exist, store the add in the vertex state
  }
  def vertexAddWithProperties(msgId:Int,srcId:Int, properties:Map[String,String]):Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2)) //add all properties
  }

  def edgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) { //local edge
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertexAdd(msgId,dstId) //do the same for the destination ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node

      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else {
        edges = edges updated((srcId,dstId),new Edge(msgId,true,srcId,dstId))
        edges(srcId,dstId) killList vertices(srcId).removeList.map(kill => kill._1) //get the remove list from source node and give to the Edge
        if(srcId!=dstId)edges(srcId,dstId) killList vertices(dstId).removeList.map(kill => kill._1) //get the remove list from destination node and give to the Edge
      } //if the edge is yet to exist
    }
    else{ //remote edge
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node

      if(edges contains (srcId,dstId)) { //if the edge already exists
        edges(srcId,dstId) revive msgId // revive
        partitionList(getPartition(dstId)) ! RemoteEdgeAdd(msgId,srcId,dstId) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Destination,getPartition(dstId))) //create the re
        val deaths = vertices(srcId).removeList.map(kill => kill._1)
        edges(srcId,dstId) killList deaths
        partitionList(getPartition(dstId)) ! RemoteEdgeAddNew(msgId,srcId,dstId,deaths) // inform the partition dealing with the destination node
      }
    }
  }

  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //don't think this is needed, may be able to remove
    edges((srcId,dstId)) revive msgId //revive the edge
  }

  def remoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,srcDeaths:List[Int]):Unit={
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //create the new edge
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge
    partitionList(getPartition(srcId)) ! RemoteReturnDeaths(msgId,srcId,dstId,deaths)
  }


  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) { //local edge
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertexAdd(msgId,dstId) //do the same for the destination ID
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
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node

      if(edges contains (srcId,dstId)) { //if the edge already exists
        edges(srcId,dstId) revive msgId // revive
        properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
        partitionList(getPartition(dstId)) ! RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Destination,getPartition(dstId))) //create the remote edge
        val deaths = vertices(srcId).removeList.map(kill => kill._1) //get the source node deaths
        edges(srcId,dstId) killList deaths //pass to teh edge
        properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
        partitionList(getPartition(dstId)) ! RemoteEdgeAddWithPropertiesNew(msgId,srcId,dstId,properties,deaths) // inform the partition dealing with the destination node
      }
    }
  }
  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //again I think this can be removed
    edges((srcId,dstId)) revive msgId //revive  the edge
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }

  def remoteEdgeAddWithPropertiesNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],srcDeaths:List[Int]):Unit={
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //create the new edge
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    partitionList(getPartition(srcId)) ! RemoteReturnDeaths(msgId,srcId,dstId,deaths)
  }


  def edgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
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
      if(!(vertices contains srcId)){ //if src vertex does not exist, create it and wipe the history so that it may contain the associated Edge list
        vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
        vertices(srcId) wipe()
      }
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node

      if(edges contains (srcId,dstId)) { //if the edge already exists
        edges((srcId,dstId)) kill msgId // kill it
        partitionList(getPartition(dstId)) ! RemoteEdgeRemoval(msgId,srcId,dstId) // inform the partition dealing with the destination node
      }
      else {
        edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Destination,getPartition(dstId))) // otherwise create and initialise as false
        val deaths = vertices(srcId).removeList.map(kill => kill._1) //get the source node deaths
        edges(srcId,dstId) killList deaths //pass to the edge
        partitionList(getPartition(dstId)) ! RemoteEdgeRemovalNew(msgId,srcId,dstId,deaths) // inform the partition dealing with the destination node
      }
    }
  }
  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!(vertices contains dstId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(dstId,new Vertex(msgId,dstId,true))
      vertices(dstId) wipe()
    }
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list

    edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
  }

  def remoteEdgeRemovalNew(msgId:Int,srcId:Int,dstId:Int,srcDeaths:List[Int]):Unit={
    if(!(vertices contains dstId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(dstId,new Vertex(msgId,dstId,true))
      vertices(dstId) wipe()
    }
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list

    edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Source,getPartition(srcId))) // otherwise create and initialise as false
    val deaths = vertices(dstId).removeList.map(kill => kill._1) //get the destination node deaths
    edges(srcId,dstId) killList srcDeaths //pass source node death lists to the edge
    edges(srcId,dstId) killList deaths  // pass destination node death lists to the edge

    partitionList(getPartition(srcId)) ! RemoteReturnDeaths(msgId,srcId,dstId,deaths)
  }

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
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
          partitionList(edges(eKey).asInstanceOf[RemoteEdge].remotePartitionID) ! RemoteEdgeRemoval(msgId, eKey._1, eKey._2)
        } //if this is the source node
        else{
          partitionList(edges(eKey).asInstanceOf[RemoteEdge].remotePartitionID) ! ReturnEdgeRemoval(msgId, eKey._1, eKey._2)
        }//if this is the dest node
      }
      log(msgId,eKey._1,eKey._2)
    })
  }

  def returnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!(vertices contains srcId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
      vertices(srcId) wipe()
    }
    vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list
    edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
  }

  def remoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,dstDeaths:List[Int]):Unit= {
    edges(srcId,dstId) killList dstDeaths
  }


  //***************** EDGE HELPERS
  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false //check if destination is also local
  def getPartition(ID:Int):Int = ID%partitionList.size //get the partition a vertex is stored in


  //*******************PRINT BLOCK
  def printToFile(entityName:String,msg: String):Unit={
    val fw:FileWriter =  if(testPartition) new FileWriter(s"testEntityLogs/$entityName.txt") else new FileWriter(s"entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }

  def log(srcId:Int):Unit = if(logging && (vertices contains srcId)) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())

  def log(msgId:Int,srcId:Int,dstId:Int):Unit={
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

}
