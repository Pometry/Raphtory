package com.raphtory.test.allcommands

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication._
import spray.json._

class AllCommandsBuilder extends GraphBuilder[String]{

  override def parseTuple(tuple:String) = {
    val command    = tuple.asInstanceOf[String]
    val parsedOBJ  = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if (commandKey.contains("VertexAdd"))
      sendUpdate(vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject))
    //else if(commandKey.contains("VertexUpdateProperties")) vertexUpdateProperties(parsedOBJ.getFields("VertexUpdateProperties").head.asJsObject)
    else if (commandKey.contains("VertexRemoval"))
      sendUpdate(vertexRemoval(parsedOBJ.getFields("VertexRemoval").head.asJsObject))
    else if (commandKey.contains("EdgeAdd"))
      sendUpdate(edgeAdd(parsedOBJ.getFields("EdgeAdd").head.asJsObject)) //if addVertex, parse to handling function
    //   else if(commandKey.contains("EdgeUpdateProperties")) edgeUpdateProperties(parsedOBJ.getFields("EdgeUpdateProperties").head.asJsObject)
    else if (commandKey.contains("EdgeRemoval"))
      sendUpdate(edgeRemoval(parsedOBJ.getFields("EdgeRemoval").head.asJsObject))
  }

  def vertexAdd(command: JsObject) = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    if (command.fields.contains("properties")) { //if there are properties within the command

      val properties = Properties(command.fields("properties").asJsObject.fields.map( pair => {  //add all of the pairs to the map
         StringProperty(pair._1, pair._2.toString())
       }).toSeq:_*)

      //send the srcID and properties to the graph manager
      VertexAddWithProperties(msgTime, srcId, properties)
    } else
      VertexAdd(msgTime, srcId)
    // if there are not any properties, just send the srcID
  }

  def vertexRemoval(command: JsObject):GraphUpdate = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    VertexDelete(msgTime, srcId)
  }

  def edgeAdd(command: JsObject):GraphUpdate = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    val dstId   = command.fields("dstID").toString().toInt //extract the dstID
    if (command.fields.contains("properties")) { //if there are properties within the command
      var properties = Properties(command.fields("properties").asJsObject.fields.map( pair => {  //add all of the pairs to the map
        StringProperty(pair._1, pair._2.toString())
      }).toSeq:_*)

      EdgeAddWithProperties(msgTime, srcId, dstId, properties)
    } else EdgeAdd(msgTime, srcId, dstId)
  }

  def edgeRemoval(command: JsObject):GraphUpdate = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    val dstId   = command.fields("dstID").toString().toInt //extract the dstID
    EdgeDelete(msgTime, srcId, dstId)
  }

}