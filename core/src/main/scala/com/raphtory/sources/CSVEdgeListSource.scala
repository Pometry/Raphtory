package com.raphtory.sources

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutBuilderSource
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.time.DateTimeParser
import com.raphtory.spouts.FileSpout
import com.raphtory.spouts.ResourceSpout

import scala.language.implicitConversions

/**
  * CSVEdgeListSource is for building graphs in Raphtory from CSV data.
  * CSV data must conform to three columns: source, target and time. Any delimiter is allowed and headers are also handled for.
  *
  * @param spout state where to ingest your data from (Mandatory field)
  * @param timeIndex state which column of your data contains the time (default index = 2)
  * @param sourceIndex state which column of your data contains the source ID (default index = 0)
  * @param targetIndex state which column of your data contains the target ID (default index = 1)
  * @param delimiter state the delimiter of your CSV data (default delimiter = ",")
  * @param header state whether your CSV data contains a header or not (default = false)
  */
class CSVEdgeListSource(
    val spout: Spout[String],
    timeIndex: Int = 2,
    sourceIndex: Int = 0,
    targetIndex: Int = 1,
    delimiter: String = ",",
    header: Boolean = false
) extends Source {
  private var typesSet: Boolean       = _
  private var dateTimeFormat: Boolean = _
  private var epochFormat: Boolean    = _
  private var longFormat: Boolean     = _
  private var stringFormat: Boolean   = _

  override def makeStream[F[_]: Async](globalIndex: Ref[F, Long]): F[fs2.Stream[F, Seq[GraphUpdate]]] =
    for {
      builderInstance <- builder.make[F]
      tuples           = if (header) spout.asStream.tail else spout.asStream
      stream           = tuples.chunks.evalMap(chunk => builderInstance.parseUpdates(chunk, globalIndex))
    } yield stream

  def buildCSVEdgeListGraph(graph: Graph, tuple: String, rawTime: String, source: String, target: String): Unit = {
    val timestamp = {
      if (dateTimeFormat)
        graph.parseDatetime(rawTime)
      else if (epochFormat)
        rawTime.toLong
      else
        throw new RuntimeException(
                s"Timestamp in tuple '$tuple' does not conform to what was seen in first" +
                  s" line of data (${if (dateTimeFormat) "datetime format" else "epoch format"})"
        )
    }

    val src = {
      if (longFormat)
        source.toLong
      else if (stringFormat)
        assignID(source)
      else
        throw new RuntimeException(s"Source does not conform to what was seen in first line of data in tuple: $tuple")
    }

    val dst = {
      if (longFormat)
        target.toLong
      else if (stringFormat)
        assignID(target)
      else
        throw new RuntimeException(s"Target does not conform to what was seen in first line of data in tuple: $tuple")
    }

    graph.addVertex(timestamp, src, Properties(ImmutableString("name", source)))
    graph.addVertex(timestamp, dst, Properties(ImmutableString("name", target)))
    graph.addEdge(timestamp, src, dst)
  }

  def checkTypesAndBuildGraph(graph: Graph, tuple: String, rawTime: String, source: String, target: String) = {
    //Check time and convert to correct type
    if (!typesSet)
      typesSet.synchronized {
        if (!typesSet) {
          try {
            rawTime.toLong
            epochFormat = true
          }
          catch {
            case e: NumberFormatException =>
              graph.parseDatetime(rawTime)
              dateTimeFormat = true
          }

          try {
            source.toLong
            target.toLong
            longFormat = true
          }
          catch {
            case e: NumberFormatException =>
              stringFormat = true
          }
        }
        typesSet = true
      }

    //    Build Graph
    buildCSVEdgeListGraph(graph, tuple, rawTime, source, target)
  }

  def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {
      val fileLine = tuple.split(delimiter).map(_.trim)
      val source   = fileLine(sourceIndex)
      val target   = fileLine(targetIndex)
      val rawTime  = fileLine(timeIndex)
      checkTypesAndBuildGraph(graph, tuple, rawTime, source, target)
    }
}

object CSVEdgeListSource {

  def apply(
      spout: Spout[String],
      timeIndex: Int = 2,
      sourceIndex: Int = 0,
      targetIndex: Int = 1,
      delimiter: String = ",",
      header: Boolean = false
  ) = new CSVEdgeListSource(spout, timeIndex, sourceIndex, targetIndex, delimiter, header)

  def fromFile(
      path: String,
      timeIndex: Int = 2,
      sourceIndex: Int = 0,
      targetIndex: Int = 1,
      delimiter: String = ",",
      header: Boolean = false
  ) = new CSVEdgeListSource(FileSpout(path), timeIndex, sourceIndex, targetIndex, delimiter, header)

  def fromResource(
      path: String,
      timeIndex: Int = 2,
      sourceIndex: Int = 0,
      targetIndex: Int = 1,
      delimiter: String = ",",
      header: Boolean = false
  ) = new CSVEdgeListSource(ResourceSpout(path), timeIndex, sourceIndex, targetIndex, delimiter, header)

}
