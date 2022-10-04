package com.raphtory.api.input.sources

import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.{Graph, GraphBuilder, ImmutableProperty, Properties, Source, SourceInstance, Spout}
import com.raphtory.spouts.{FileSpout, IdentitySpout}
import com.raphtory.internals.time.DateTimeParser.{defaultParse => parseDateTime}


class CSVEdgeListSource(override val spout: Spout[String]) extends Source {
  override type MessageType = String

  var dateTimeFormat: Boolean = _
  var epochFormat: Boolean = _

  var longFormat: Boolean = _
  var stringFormat: Boolean = _

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
      case _: Throwable => throw new RuntimeException("Check format")
    }

    try {
      source.toLong
      target.toLong
      longFormat = true
    } catch {
      case e: NumberFormatException =>
        stringFormat = true
      case _: Throwable => throw new RuntimeException("Check format of source and target")
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
