package com.raphtory.api.input

import com.raphtory.api.input.Graph.assignID
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.spouts.FileSpout
import com.twitter.chill.ClosureCleaner
import io.circe._
import io.circe.parser._
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

trait Source {
  type MessageType
  def spout: Spout[MessageType]
  def builder: GraphBuilder[MessageType]

  def buildSource(graphID: String, id: Int): SourceInstance[MessageType]
  def getBuilderClass: Class[_] = builder.getClass
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

  def buildSource(graphID: String, id: Int): SourceInstance[T] =
    new SourceInstance[T](id, spout.buildSpout(), builder.buildInstance(graphID, id))
}

class CSVEdgeListSource(override val spout: Spout[String]) extends Source {
  override type MessageType = String

  var dateTimeFormat: Boolean = _
  var epochFormat: Boolean = _

  private var timeIndex = 2
  private var sourceIndex = 0
  private var targetIndex = 1

  def setIndexPositions(timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1) = {
    this.timeIndex = timeIndex
    this.sourceIndex = sourceIndex
    this.targetIndex = targetIndex
    this
  }
  def buildCSVEdgeListGraph(graph: Graph, rawTime: String, source: String, target: String) = {
    val timestamp =
    if (dateTimeFormat) {
      parseDateTime(rawTime)
    } else if (epochFormat) {
      rawTime.toLong
    } else {
      throw new RuntimeException("Check timestamp is in Epoch or DateTime Format")
    }
    graph.addVertex(timestamp, source, Properties(ImmutableProperty("name", source)))
    graph.addVertex(timestamp, target, Properties(ImmutableProperty("name", target)))
    graph.addEdge(timestamp, source, target)
  }
  def checkTypesAndBuildGraph(graph: Graph, rawTime: String, source: String, target: String) = {
    //Check time and convert to correct type
      try {
        rawTime.toLong
        epochFormat = true
      } catch {
        case e: NumberFormatException =>
          parseDateTime(rawTime)
          dateTimeFormat = true
        case _: Throwable => throw new RuntimeException("Check format")
      }
//    Build Graph
    buildCSVEdgeListGraph(graph, rawTime, source, target)
  }
  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {
        val fileLine = tuple.split(",").map(_.trim)
        val source = fileLine(sourceIndex)
        val target = fileLine(targetIndex)
        val rawTime = fileLine(timeIndex)
        if (graph.index == 1) {
          checkTypesAndBuildGraph(graph, rawTime, source, target)
        } else {
          buildCSVEdgeListGraph(graph, rawTime, source, target)
        }
    }
  def buildSource(graphID: String, id: Int): SourceInstance[String] =
    new SourceInstance[String](id, spout.buildSpout(), builder.buildInstance(graphID, id))

}

object CSVEdgeListSource {
  def apply(path: String) = new CSVEdgeListSource(FileSpout(path))
  def apply(spout: Spout[String]) = new CSVEdgeListSource(spout)
}

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
      timestamp = parseDateTime(timeCursor.toString)
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
    val timeCursor = cursor.downField(time)

    try {
      timeCursor.as[Long]
      epochFormat = true
    } catch {
      case e: NumberFormatException =>
        parseDateTime(timeCursor.as[String].toString)
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
    buildEdgeListGraphFromJSON(graph, timeCursor, sourceCursor, targetCursor)
  }
  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {
      if (graph.index == 1) {
        checkAllTypesAndBuildGraph(tuple, graph)
      } else {
        val json: Json = parse(tuple).getOrElse(Json.Null)
        val cursor: HCursor = json.hcursor
        val timeCursor = cursor.downField(time)
        val sourceCursor = cursor.downField(src)
        val targetCursor = cursor.downField(dst)
        buildEdgeListGraphFromJSON(graph, timeCursor, sourceCursor, targetCursor)
      }
    }

  override def buildSource(graphID: String, id: Int): SourceInstance[String] =
    new SourceInstance[String](id, spout.buildSpout(), builder.buildInstance(graphID, id))
}

object JSONEdgeListSource {
  def apply(path:String, time: String, src: String, dst: String) = new JSONEdgeListSource(FileSpout(path), time, src, dst)
  def apply(spout: Spout[String], time: String, src: String, dst: String) = new JSONEdgeListSource(spout, time, src, dst)
}

class SourceInstance[T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderInstance[T]) {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  def sourceID: Int  = id

  def hasRemainingUpdates: Boolean = spoutInstance.hasNext

  def sendUpdates(index: Long, failOnError: Boolean): Unit = {
    val element = spoutInstance.next()
    builderInstance.sendUpdates(element, index)(failOnError)
  }

  def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()

  def executeReschedule(): Unit = spoutInstance.executeReschedule()

  def setupStreamIngestion(
      streamWriters: collection.Map[Int, EndPoint[GraphAlteration]]
  ): Unit =
    builderInstance.setupStreamIngestion(streamWriters)

  def close(): Unit = spoutInstance.close()

  def sentMessages(): Long = builderInstance.getSentUpdates

  def highestTimeSeen(): Long = builderInstance.highestTimeSeen
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
