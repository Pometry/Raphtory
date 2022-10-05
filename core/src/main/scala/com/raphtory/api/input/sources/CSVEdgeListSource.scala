package com.raphtory.api.input.sources

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{Graph, GraphBuilder, ImmutableProperty, Properties, Source, SourceInstance, Spout, SpoutInstance}
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.spouts.{FileSpout, IdentitySpout}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}

import scala.language.implicitConversions

class CSVEdgeListSource(override val spout: Spout[String], timeIndex: Int, sourceIndex: Int, targetIndex: Int) extends Source {
  override type MessageType = String

  private var dateTimeFormat: Boolean = _
  private var epochFormat: Boolean = _

  private var longFormat: Boolean = _
  private var stringFormat: Boolean = _


  def buildCSVEdgeListGraph(graph: Graph, rawTime: String, source: String, target: String) = {
    val timestamp = {
      if (dateTimeFormat) {
        parseDateTime(rawTime)
      } else if (epochFormat) {
        rawTime.toLong
      } else {
        throw new RuntimeException("Check timestamp is in Epoch or DateTime Format")
      }
    }

    val src = {
      if (longFormat) {
        source.toLong
      } else if (stringFormat) {
        assignID(source)
      } else {
        throw new RuntimeException("Check source is in Long or String format")
      }
    }

    val dst = {
      if (longFormat) {
        target.toLong
      } else if (stringFormat) {
        assignID(target)
      } else {
        throw new RuntimeException("Check source is in Long or String format")
      }
    }

    graph.addVertex(timestamp, src, Properties(ImmutableProperty("name", source)))
    graph.addVertex(timestamp, dst, Properties(ImmutableProperty("name", target)))
    graph.addEdge(timestamp, src, dst)
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
      case e: Throwable => throw new RuntimeException(s"$e")
    }

    try {
      source.toLong
      target.toLong
      longFormat = true
    } catch {
      case e: NumberFormatException =>
        stringFormat = true
      case e: Throwable => throw new RuntimeException(s"$e: Check format of source and target")
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
    new SourceInstance[String](id, spout.buildSpout(), new GraphBuilderInstance(graphID, id, builder))

}

object CSVEdgeListSource {
  def apply(spout: Spout[String], timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1) = new CSVEdgeListSource(spout, timeIndex, sourceIndex, targetIndex)
  implicit def convert(fromFile: String, timeIndex: Int = 2, sourceIndex: Int = 0, targetIndex: Int = 1) =  new CSVEdgeListSource(FileSpout(fromFile), timeIndex, sourceIndex, targetIndex)
}
