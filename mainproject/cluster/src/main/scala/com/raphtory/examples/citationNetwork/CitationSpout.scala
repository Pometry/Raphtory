package com.raphtory.examples.citationNetwork

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class CitationSpout extends SpoutTrait {

  //file is read. Please note that the first line is dropped, this in case the file has headers
  val fileLines = io.Source.fromFile("/Users/lagordamotoneta/Scala/small.csv").getLines.drop(1).toArray
  var position=0
  var linesNumber=fileLines.length
  println(linesNumber)

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1,MILLISECONDS),"newLine")
    case "newLine" => {
      if (position<linesNumber) {
        var line = fileLines(position)
        sendTuple(line)
        position += 1
        AllocateSpoutTask( Duration(1, MILLISECONDS), "newLine")
      }
    }
    case _ => println("message not recognized!")
  }

}

