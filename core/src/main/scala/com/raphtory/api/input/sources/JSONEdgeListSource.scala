package com.raphtory.api.input.sources

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{Graph, GraphBuilder, Source, SourceInstance, Spout}
import com.raphtory.spouts.FileSpout
import io.circe.{ACursor, Decoder, HCursor, Json}
import io.circe.parser.parse
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

class JSONEdgeListSource(override val spout: Spout[String], time: String, src: String, dst: String) extends Source {
  override type MessageType = String

  var dateTimeFormat: Boolean = false
  var epochFormat: Boolean = false
  var sourceIsLong: Boolean = false
  var targetIsLong: Boolean = false
  var sourceIsString: Boolean = false
  var targetIsString: Boolean = false
  var timestamp: Long = _
  var sourceID: Long = _
  var targetID: Long = _

  def buildEdgeListGraphFromJSON(graph: Graph, timeCursor: ACursor, sourceCursor: ACursor, targetCursor: ACursor) = {
    if (epochFormat) {
      timestamp = timeCursor.as[Long].toString.toLong
    } else if (dateTimeFormat) {
      timestamp = parseDateTime(timeCursor.as[String].toString)
      println(timestamp)
    } else {
      throw new RuntimeException("Cannot create timestamp")
    }

    if (sourceIsLong) {
      sourceID = sourceCursor.as[Long].toString.toLong
    } else if (sourceIsString) {
      sourceID = assignID(sourceCursor.as[String].toString)
    } else {
      throw new RuntimeException("Cannot create source ID")
    }

    if (targetIsLong) {
      targetID = targetCursor.as[Long].toString.toLong
    } else if (targetIsString) {
      targetID = assignID(timeCursor.as[String].toString)
    } else {
      throw new RuntimeException("Cannot create target ID")
    }

    graph.addVertex(timestamp, sourceID)
    graph.addVertex(timestamp, targetID)
    graph.addEdge(timestamp, sourceID, targetID)
  }

  def checkAllTypesAndBuildGraph(tuple: String, graph: Graph) = {
    // Get JSON and cursor
    val json: Json = parse(tuple).getOrElse(Json.Null)
    val cursor: HCursor = json.hcursor
    //Check Time
    val timeCursor = cursor.downField("items")
    val test= timeCursor.downArray.downField("appointed_on")
    println(test.as[String])
    try {
      test.as[Long]
      epochFormat = true
    } catch {
      case e: NumberFormatException =>
        parseDateTime(test.as[String].toString)
        println("dateformat")
        dateTimeFormat = true
      case _: Throwable => throw new RuntimeException("Make sure timestamp is in Epoch or DateTime format.")
    }

    //Check Source
    val sourceCursor = cursor.downField(src)

    sourceCursor.as[String] match {
      case _: Decoder.Result[String] => sourceIsString = true
      case _ => sourceCursor.as[Long].toString.toLong match {
        case _: Long => sourceIsLong = true
        case _ => throw new RuntimeException("Source is neither String or Long, check types")
      }
    }
    //target
    val targetCursor = cursor.downField(dst)
    targetCursor.as[String] match {
      case _: Decoder.Result[String] => targetIsString = true
      case _ => targetCursor.as[Long].toString.toLong match {
        case _: Long => targetIsLong = true
        case _ => throw new RuntimeException("Target is neither String or Long, check types")
      }
    }

    //Build Graph
    buildEdgeListGraphFromJSON(graph, test, sourceCursor, targetCursor)
  }
  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {
      if (graph.index == 1) {
        checkAllTypesAndBuildGraph(tuple, graph)
      } else {
        val json: Json = parse(tuple).getOrElse(Json.Null)
        val cursor: HCursor = json.hcursor
        val timeCursor = cursor.downField("items")
        val test = timeCursor.downArray.downField("appointed_on")
        val sourceCursor = cursor.downField(src)
        val targetCursor = cursor.downField(dst)
        buildEdgeListGraphFromJSON(graph, test, sourceCursor, targetCursor)
      }
    }

  override def buildSource(graphID: String, id: Int): SourceInstance[String] =
    new SourceInstance[String](id, spout.buildSpout(), builder.buildInstance(graphID, id))
}

object JSONEdgeListSource {
  def apply(path:String, time: String, src: String, dst: String) = new JSONEdgeListSource(FileSpout(path), time, src, dst)
  def apply(spout: Spout[String], time: String, src: String, dst: String) = new JSONEdgeListSource(spout, time, src, dst)
}