package com.raphtory.examples.gab.actors

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class GabExampleSpout extends SpoutTrait {


  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/app").trim
  val file_name = System.getenv().getOrDefault("GAB_FILE_NAME", "gabNetwork500.csv").trim
  val fileLines = io.Source.fromFile(directory+"/"+file_name).getLines.drop(1).toArray
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
            for(i <- 1 to 100){
              var line = fileLines(position)
              sendCommand(line)
              position += 1
            }
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

}


