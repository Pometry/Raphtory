package com.raphtory.api.input.sources

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{Graph, GraphBuilder, ImmutableProperty, Properties, Source, Spout}
import com.raphtory.spouts.{FileSpout, ResourceSpout}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

import scala.language.implicitConversions

class CSVEdgeListSource(override val spout: Spout[String], timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1, delimiter: String = ",", header: Boolean = false) extends Source {
  override type MessageType = String

  private var dateTimeFormat: Boolean = _
  private var epochFormat: Boolean = _

  private var longFormat: Boolean = _
  private var stringFormat: Boolean = _


  def buildCSVEdgeListGraph(graph: Graph, tuple: String, rawTime: String, source: String, target: String) = {
    val timestamp = {
      if (dateTimeFormat) {
        parseDateTime(rawTime)
      } else if (epochFormat) {
        rawTime.toLong
      } else {
        throw new RuntimeException(s"Timestamp does not conform to what was seen in first line of data in tuple: $tuple")
      }
    }

    val src = {
      if (longFormat) {
        source.toLong
      } else if (stringFormat) {
        assignID(source)
      } else {
        throw new RuntimeException(s"Source does not conform to what was seen in first line of data in tuple: $tuple")
      }
    }

    val dst = {
      if (longFormat) {
        target.toLong
      } else if (stringFormat) {
        assignID(target)
      } else {
        throw new RuntimeException(s"Target does not conform to what was seen in first line of data in tuple: $tuple")
      }
    }

    graph.addVertex(timestamp, src, Properties(ImmutableProperty("name", source)))
    graph.addVertex(timestamp, dst, Properties(ImmutableProperty("name", target)))
    graph.addEdge(timestamp, src, dst)
  }
  def checkTypesAndBuildGraph(graph: Graph, tuple: String, rawTime: String, source: String, target: String) = {
    //Check time and convert to correct type
    try {
      rawTime.toLong
      epochFormat = true
    } catch {
      case e: NumberFormatException =>
        parseDateTime(rawTime)
        dateTimeFormat = true
    }

    try {
      source.toLong
      target.toLong
      longFormat = true
    } catch {
      case e: NumberFormatException =>
        stringFormat = true
    }
    //    Build Graph
    buildCSVEdgeListGraph(graph, tuple, rawTime, source, target)
  }
  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {

      val fileLine = tuple.split(delimiter).map(_.trim)
      val source = fileLine(sourceIndex)
      val target = fileLine(targetIndex)
      val rawTime = fileLine(timeIndex)
      graph.index match {
        case 1 => if (!header) checkTypesAndBuildGraph(graph, tuple, rawTime, source, target)
        case 2 => if (header) checkTypesAndBuildGraph(graph, tuple, rawTime, source, target) else buildCSVEdgeListGraph(graph, tuple, rawTime, source, target)
        case _ => buildCSVEdgeListGraph(graph,tuple, rawTime, source, target)
      }
    }

}

object CSVEdgeListSource {
  def apply(spout: Spout[String], timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1, delimiter: String = ",", header: Boolean = false) = new CSVEdgeListSource(spout, timeIndex, sourceIndex, targetIndex, delimiter, header)
  def fromFile(path: String, timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1, delimiter: String = ",", header: Boolean = false) = new CSVEdgeListSource(FileSpout(path), timeIndex, sourceIndex, targetIndex, delimiter, header)
  def fromResource(path: String, timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1, delimiter: String = ",", header: Boolean = false) = new CSVEdgeListSource(ResourceSpout(path), timeIndex, sourceIndex, targetIndex, delimiter, header)

}
