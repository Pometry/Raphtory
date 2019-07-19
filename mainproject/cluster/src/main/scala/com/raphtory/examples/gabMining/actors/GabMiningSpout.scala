package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class GabMiningSpout extends SpoutTrait {

  //file is read. Please note that the first line is dropped, this in case the file has headers
  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/Users/lagordamotoneta/Documents/QMUL/QMUL/project/Datasets").trim
  val fileLines = io.Source.fromFile(directory+"/gabNetwork1000.csv").getLines.drop(1).toArray
  var position = 0
  var linesNumber=fileLines.length
  println(linesNumber)

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()

    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(100, NANOSECONDS), self, "newLine")

  }

  protected def processChildMessages(message: Any): Unit = {
    if (position<linesNumber) {

      message match {
        case "newLine" => {
          if (isSafe()) {
           // println(fileLines(position))
            var line = fileLines(position)
            sendCommand(line)
            position += 1
          }
        }
        case "stop" => stop()
        case _ => println("message not recognized!")
      }
    }

    else{
      stop()
    }
  }


  def running(): Unit = {
    //genRandomCommands(totalCount)
    //totalCount+=1000
  }

}


