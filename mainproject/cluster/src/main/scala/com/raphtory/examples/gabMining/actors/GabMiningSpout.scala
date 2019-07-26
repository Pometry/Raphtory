package com.raphtory.examples.gabMining.actors

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps

class GabMiningSpout extends SpoutTrait {
//  2016-08-17T18:40:20+00:00;3509;752;;3504;292
//  2016-08-17T18:46:56+00:00;3512;810;;3440;741
//  2016-08-17T18:48:37+00:00;3514;810;;3444;741
//  2016-08-17T19:32:21+00:00;3573;721;;3565;31
//  2016-08-17T19:46:22+00:00;3595;759;;3587;709
//  2016-08-17T19:47:06+00:00;3596;759;;3585;817

  //file is read. Please note that the first line is dropped, this in case the file has headers
  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/Users/lagordamotoneta/Documents/QMUL/QMUL/project/Datasets").trim
  val fileLines = io.Source.fromFile(directory+"/gabNetwork500.csv").getLines.drop(1).toArray
  var position = 0
  var linesNumber=fileLines.length
  println(fileLines.map(_.split(";")(2).trim.toInt).toSet.union(fileLines.map(_.split(";")(5).trim.toInt).toSet).size)
  println(fileLines.map(_.split(";")(2).trim.toInt).contains(-1))

  println(linesNumber)

  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()

    context.system.scheduler.schedule(Duration(1, SECONDS), Duration(100, NANOSECONDS), self, "newLine")

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


