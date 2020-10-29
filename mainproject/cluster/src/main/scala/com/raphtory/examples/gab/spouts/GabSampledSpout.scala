package com.raphtory.examples.gab.spouts

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.components.Spout.SpoutTrait.BasicDomain
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.Next
import com.raphtory.core.model.communication.StringSpoutGoing

import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

class GabSampledSpout extends SpoutTrait[BasicDomain,StringSpoutGoing] {

  val prop = 1.0/10
  val r = new Random()

  val directory = System.getenv().getOrDefault("GAB_DIRECTORY", "/app").trim
  val file_name = System.getenv().getOrDefault("GAB_FILE_NAME", "gab.csv").trim
  val fileLines = Source.fromFile(directory + "/" + file_name).getLines.drop(1).toArray
  // upstream/master
  var position    = 0
  var linesNumber = fileLines.length
  println("Start: " + LocalDateTime.now())

  override def handleDomainMessage(message: BasicDomain): Unit = message match {
    case Next =>
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
                sendTuple(StringSpoutGoing(line))
                position += 1
              }
            }
          }
          self ! Next
        }
        else
        {
          println("Finished ingestion")
        }
      }
      catch {case e:Exception => println("Finished ingestion")}
    case _ => println("message not recognized!")
  }

  override def startSpout(): Unit = self ! Next

}
