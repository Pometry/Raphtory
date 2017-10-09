package com.gwz.dockerexp.caseclass

import akka.actor.ActorRef
import com.gwz.dockerexp.GraphEntities.{Edge, Vertex}

/**
  * Created by Mirate on 30/05/2017.
  */
class raphtoryCases {}



//The following block are all case classes (commands) which the manager can handle
case class BenchmarkUpdate(id:Int, updateID:Int, count:Int)

case class LiveAnalysis()
case class Results(result:String)

case class VertexAdd(msgId:Int,srcId:Int) //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(msgId:Int,srcId:Int, properties: Map[String,String])
case class VertexUpdateProperties(msgId:Int,srcId:Int, propery:Map[String,String])
case class VertexRemoval(msgId:Int,srcId:Int)

case class EdgeAdd(msgId:Int,srcId:Int,destId:Int)
case class EdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int, properties: Map[String,String])
case class EdgeUpdateProperties(msgId:Int,srcId:Int,dstId:Int,property:Map[String,String])
case class EdgeRemoval(msgId:Int,srcId:Int,dstID:Int)

case class RemoteEdgeUpdateProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String])
case class RemoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int)
case class RemoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties: Map[String,String])
case class RemoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int)

case class RemoteEdgeUpdatePropertiesNew(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String],kills:List[Int])
case class RemoteEdgeAddNew(msgId:Int,srcId:Int,dstId:Int,kills:List[Int])
case class RemoteEdgeAddWithPropertiesNew(msgId:Int,srcId:Int,dstId:Int,properties: Map[String,String],kills:List[Int])
case class RemoteEdgeRemovalNew(msgId:Int,srcId:Int,dstId:Int,kills:List[Int])

case class RemoteReturnDeaths(msgId:Int,srcId:Int,dstId:Int,kills:List[Int])
case class ReturnEdgeRemoval(msgId:Int,srcId:Int,dstId:Int)