package com.raphtory.examples.enron.graphbuilders

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.ImmutableProperty
import com.raphtory.api.input.Properties
import com.raphtory.api.input.Type

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.ZoneOffset

class EnronGraphBuilder() extends GraphBuilder[String] {

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
  override def parseTuple(tuple: String): Unit = {

    val dateFormat = "EEE, d MMM yyyy HH:mm:ss Z (z)"
    val sParse     = "*Line : " + tuple

    val msgId    = sParse.split("Date:")(0).trim
    val date     = sParse.split("Date:")(1).split("From:")(0).trim
    val sender   = sParse.split("From:")(1).split("To:")(0).trim
    val receiver = sParse.split("To:")(1).split("Subject:")(0).trim
    val subject  = Option(sParse.split("Subject:")(1).split("Mime-Version:")(0).trim)

    //Extract timestamp from second column
    var dateEpochUTC: Long = 0
    val parseFormatter     = DateTimeFormatter.ofPattern(dateFormat)
    val dateTime           = LocalDateTime.parse(date, parseFormatter)
    dateEpochUTC = dateTime.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli

    val srcID = assignID(sender)
    val tarID = assignID(receiver)

    addVertex(dateEpochUTC, srcID, Properties(ImmutableProperty("name", sender)), Type("Character"))
    addVertex(
            dateEpochUTC,
            tarID,
            Properties(ImmutableProperty("name", receiver)),
            Type("Character")
    )
    addEdge(
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
}
