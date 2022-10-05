package com.raphtory.enrontest

import com.raphtory.api.input.{Graph, GraphBuilder, Source, SourceInstance, Spout}
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.api.input.Graph.assignID
import com.raphtory.spouts.FileSpout

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

class ENRONSource(override val spout: Spout[String]) extends Source {
  override type MessageType = String

  override def builder: GraphBuilder[String] =
    (graph: Graph, tuple: String) => {

      val dateFormat = "EEE, d MMM yyyy HH:mm:ss Z (z)"
      val sParse = "*Line : " + tuple

      val msgId = sParse.split("Date:")(0).trim
      val date = sParse.split("Date:")(1).split("From:")(0).trim
      val sender = sParse.split("From:")(1).split("To:")(0).trim
      val receiver = sParse.split("To:")(1).split("Subject:")(0).trim
      val subject = Option(sParse.split("Subject:")(1).split("Mime-Version:")(0).trim)

      //    logger.trace(
      //            s"Parsed record.  msgId: '$msgId' date: '$date' sender: '$sender' receiver: '$receiver'."
      //    )

      //Extract timestamp from second column
      var dateEpochUTC: Long = 0
      val parseFormatter = DateTimeFormatter.ofPattern(dateFormat)
      val dateTime = LocalDateTime.parse(date, parseFormatter)
      dateEpochUTC = dateTime.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
      //          logger.trace(s"Parsed date field into epoch '$dateEpochUTC'.")

      val srcID = assignID(sender)
      val tarID = assignID(receiver)

      graph.addVertex(dateEpochUTC, srcID, Properties(ImmutableProperty("name", sender)), Type("Character"))
      graph.addVertex(
        dateEpochUTC,
        tarID,
        Properties(ImmutableProperty("name", receiver)),
        Type("Character")
      )
      graph.addEdge(
        dateEpochUTC,
        srcID,
        tarID,
        Properties(
          ImmutableProperty("MsgId", msgId),
          ImmutableProperty("Subject", subject.get)
        ),
        Type("Character")
      )
    }

  def buildSource(graphID: String, id: Int): SourceInstance[String] =
    new SourceInstance[String](id, spout.buildSpout(), builder.buildInstance(graphID, id))

}

object ENRONSource {
  def apply(path: String) = new ENRONSource(FileSpout(path))
  def apply(spout: Spout[String]) = new ENRONSource(spout)
}
