package com.raphtory.stateTest

import com.raphtory.api.input.FloatProperty
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Properties._

import spray.json._

object AllCommandsBuilder extends GraphBuilder[String] {

  def apply(graph: Graph, command: String) = {
    val parsedOBJ  = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields             //get the command type
    if (commandKey.contains("VertexAdd"))
      vertexAdd(graph, parsedOBJ.getFields("VertexAdd").head.asJsObject)
    else if (commandKey.contains("VertexRemoval"))
      vertexRemoval(graph, parsedOBJ.getFields("VertexRemoval").head.asJsObject)
    else if (commandKey.contains("EdgeAdd"))
      edgeAdd(graph, parsedOBJ.getFields("EdgeAdd").head.asJsObject) //if addVertex, parse to handling function
    else if (commandKey.contains("EdgeRemoval"))
      edgeRemoval(graph, parsedOBJ.getFields("EdgeRemoval").head.asJsObject)
  }

  def vertexAdd(graph: Graph, command: JsObject): Unit = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    if (command.fields.contains("properties")) { //if there are properties within the command

      val properties = Properties(
              command
                .fields("properties")
                .asJsObject
                .fields
                .map { pair => //add all of the pairs to the map
                  FloatProperty(pair._1, pair._2.toString().toFloat)
                }
                .toSeq: _*
      )

      //send the srcID and properties to the graph manager
      graph.addVertex(msgTime, srcId, properties)
    }
    else
      graph.addVertex(msgTime, srcId)
    // if there are not any properties, just send the srcID
  }

  def vertexRemoval(graph: Graph, command: JsObject): Unit = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    graph.deleteVertex(msgTime, srcId)
  }

  def edgeAdd(graph: Graph, command: JsObject): Unit = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    val dstId   = command.fields("dstID").toString().toInt //extract the dstID
    if (command.fields.contains("properties")) { //if there are properties within the command
      val properties = Properties(
              command
                .fields("properties")
                .asJsObject
                .fields
                .map { pair => //add all of the pairs to the map
                  FloatProperty(pair._1, pair._2.toString().toFloat)
                }
                .toSeq: _*
      )

      graph.addEdge(msgTime, srcId, dstId, properties)
    }
    else
      graph.addEdge(msgTime, srcId, dstId)
  }

  def edgeRemoval(graph: Graph, command: JsObject): Unit = {
    val msgTime = command.fields("messageID").toString().toLong
    val srcId   = command.fields("srcID").toString().toInt //extract the srcID
    val dstId   = command.fields("dstID").toString().toInt //extract the dstID
    graph.deleteEdge(msgTime, srcId, dstId)
  }

}
