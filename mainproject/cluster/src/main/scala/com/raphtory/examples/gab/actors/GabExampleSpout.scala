package com.raphtory.examples.gab.actors

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class GabExampleSpout extends SpoutTrait {

  //file is read. Please note that the first line is dropped, this in case the file has headers
  //the directories of where the datasets are located are initialised. Either by harcoding the value or by setting
  // an environment variable in the system. The fle is read line by line , and the line is sent to the Router to
  // be further processed.
  // In the case of this spout, Akka scheduler is used control the number of lines read and sent to the router with an
  //scheduler.
  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/Users/lagordamotoneta/Documents/QMUL/QMUL/project/Datasets").trim



  val fileLines = io.Source.fromFile(directory+"/gabNetwork500.csv").getLines.drop(1).toArray
// upstream/master
  var position = 0
  var linesNumber=fileLines.length
  println("Start: "+ LocalDateTime.now())
  println("Vertices Users "+fileLines.map(_.split(";")(2).trim.toInt).toSet.union(fileLines.map(_.split(";")(5).trim.toInt).toSet).size)
  println("Vertices Comments "+fileLines.map(_.split(";")(1).trim.toInt).toSet.union(fileLines.map(_.split(";")(4).trim.toInt).toSet).size)
  //println("2 "+fileLines.map(_.split(";")(5).trim.toInt).contains(-1) )
  println("Edges "+fileLines.filter(line => line.contains("-1")).length )


  println("Lines "+linesNumber)

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()

    context.system.scheduler.schedule(Duration(1, SECONDS), Duration(100, NANOSECONDS), self, "newLine")

  }

  protected def processChildMessages(message: Any): Unit = {
    if (position<linesNumber) {
      message match {
        case "newLine" => {
          if (isSafe()) {
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


