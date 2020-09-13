package com.raphtory.examples.tsvnet

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.duration.{Duration, NANOSECONDS}
import scala.io
import scala.util.Random
import scala.util.control.Breaks._

class SamplerSpout extends SpoutTrait {

  val prop = 1.0/10
  val r = new Random()
  val directory = System.getenv().getOrDefault("TSV_DIRECTORY", "/app").trim
  val file_name = System.getenv().getOrDefault("TSV_FILE_NAME", "sx_reordered.txt").trim
  val fileLines = io.Source.fromFile(directory + "/" + file_name).getLines.drop(1).toArray
  // upstream/master
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
    case "newLine" =>
      try {
        if (position < linesNumber) {
          for (i <- 1 to 100) {
            val randFloat = r.nextFloat()
            breakable {
              if (randFloat > prop) {
                position +=1
                break
              } else {
                val line = fileLines(position)
                sendTuple(line)
                position += 1
              }
            }
          }
            AllocateSpoutTask(Duration(1, NANOSECONDS), "newLine")
        }
          else
          {
            println("Finished ingestion")
          }
        }
      catch {case e:Exception => println("Finished ingestion")}
    case _ => println("message not recognized!")
  }

}
