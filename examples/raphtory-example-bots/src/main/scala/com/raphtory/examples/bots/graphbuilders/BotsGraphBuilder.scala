package com.raphtory.examples.bots.graphbuilders

import com.raphtory.api.input.{FloatProperty, GraphBuilder, ImmutableProperty, Properties, StringProperty, Type}

import java.text.SimpleDateFormat
import java.time.OffsetDateTime
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
//    val line = new String(tuple,"UTF-8")//author_id	created_at	in_reply_to_user_id	lang	retweet_count	like_count	source	text
    val fileLine   = tuple.split(",").map(_.trim)
    val userID  = fileLine(1).toLong
    val replyID = fileLine(2).toLong
    val timestamp  = getTimeStamp(fileLine(0)).toLong()
    var sentiment = "None"
    if (fileLine.length >= 3) :
      val sentiment = fileLine(3).toString
//    val timestamp = OffsetDateTime.parse(fileLine(1)).toEpochSecond
    addVertex(timestamp, userID, Type("User"))
    if (replyID != 0L) {
      addVertex(timestamp, replyID.toFloat.toLong, Type("User"))
      addEdge(timestamp, userID, replyID.toFloat.toLong, Properties(ImmutableProperty("sentiment", sentiment)),Type("Retweeted"))
    }
  }

}
