package com.raphtory.enrontest

import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type
import com.raphtory.api.input.Graph.assignID

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

object ENRONGraphBuilder extends GraphBuilder[String] {

  /*
   * Data has structure (2 fields):
   * Col1: "file"
   * Col2: "message"
   * Sample record:
   *
    "allen-p/_sent_mail/1.","Message-ID: <18782981.1075855378110.JavaMail.evans@thyme>
    Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)
    From: phillip.allen@enron.com
    To: tim.belden@enron.com
    Subject:
    Mime-Version: 1.0
    Content-Type: text/plain; charset=us-ascii
    Content-Transfer-Encoding: 7bit
    X-From: Phillip K Allen
    X-To: Tim Belden <Tim Belden/Enron@EnronXGate>
    X-cc:
    X-bcc:
    X-Folder: \Phillip_Allen_Jan2002_1\Allen, Phillip K.\'Sent Mail
    X-Origin: Allen-P
    X-FileName: pallen (Non-Privileged).pst

    Here is our forecast"
   *
   * Parsing of ENRON is complex, before using change data set as follows:
   * To use file as input, remove all `\n` chars from file with:
   * tr -d '\n' < test_email.csv
   * Remove all " and '
   * Add `\n` wherever there is a `sent_mail` : this signifies a new record
   * */
  def apply(graph: Graph, line: String): Unit = {
    if ((line.toString.equals("")))
//      logger.warn(s"File contained an empty line.")
      return

    val dateFormat = "EEE, d MMM yyyy HH:mm:ss Z (z)"
    val sParse     = "*Line : " + line.toString

    val msgId    = sParse.split("Date:")(0).trim
    val date     = sParse.split("Date:")(1).split("From:")(0).trim
    val sender   = sParse.split("From:")(1).split("To:")(0).trim
    val receiver = sParse.split("To:")(1).split("Subject:")(0).trim
    val subject  = Option(sParse.split("Subject:")(1).split("Mime-Version:")(0).trim)

//    logger.trace(
//            s"Parsed record.  msgId: '$msgId' date: '$date' sender: '$sender' receiver: '$receiver'."
//    )

    //Extract timestamp from second column
    var dateEpochUTC: Long = 0
    val parseFormatter     = DateTimeFormatter.ofPattern(dateFormat)
    val dateTime           = LocalDateTime.parse(date, parseFormatter)
    dateEpochUTC = dateTime.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli

//    logger.trace(s"Parsed date field into epoch '$dateEpochUTC'.")

    val srcID = assignID(sender)
    val tarID = assignID(receiver)

    graph.addVertex(dateEpochUTC, srcID, Properties(ImmutableString("name", sender)), Type("Character"))
    graph.addVertex(
            dateEpochUTC,
            tarID,
            Properties(ImmutableString("name", receiver)),
            Type("Character")
    )
    graph.addEdge(
            dateEpochUTC,
            srcID,
            tarID,
            Properties(
                    ImmutableString("MsgId", msgId),
                    ImmutableString("Subject", subject.get)
            ),
            Type("Character")
    )
  }
}
