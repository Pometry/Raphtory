package com.raphtory.examples.bots.graphbuilders

import com.raphtory.api.input.{GraphBuilder, ImmutableProperty, Properties, Type}

import java.text.SimpleDateFormat
import java.util.Date

class BotsGraphBuilder extends GraphBuilder[String] {

  def getTimeStamp(dateString: String): Long = {
    val dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy")
    var date: Date = new Date()
    try {
      date = dateFormat.parse(dateString)
    } catch {
      case e: java.text.ParseException => 0
    }
    val epoch = date.getTime
    epoch/1000
  }

  override def parseTuple(tuple: String): Unit = {
//    val line = new String(tuple,"UTF-8")
    val fileLine   = tuple.split(",").map(_.trim)
    val sourceNode = fileLine(2)
    val srcID      = assignID(sourceNode)
    val targetNode = fileLine(9)
    val tarID      = assignID(targetNode)
    val timeStamp  = getTimeStamp(fileLine(1))

    addVertex(
            timeStamp,
            srcID,
            Properties(ImmutableProperty("name", sourceNode)),
            Type("User")
    )
    addVertex(
            timeStamp,
            tarID,
            Properties(ImmutableProperty("name", targetNode)),
            Type("User")
    )
    addEdge(timeStamp, srcID, tarID, Type("Retweet"))
  }

}
