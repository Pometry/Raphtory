package com.raphtory.examples.ldbc.spouts

import java.time.LocalDateTime

import com.raphtory.core.components.Spout.SpoutTrait

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io
import scala.language.postfixOps
import scala.concurrent.duration.{Duration, NANOSECONDS}

class LDBCSpout extends SpoutTrait {


  val directory = System.getenv().getOrDefault("LDBC_DIRECTORY", "/Users/mirate/Documents/phd/ldbc_snb_datagen/social_network/dynamic").trim

  val peopleFile = io.Source.fromFile(directory+"/"+"person_0_0.csv").getLines.drop(1).toArray
  val friendFile = io.Source.fromFile(directory+"/"+"person_knows_person_0_0.csv").getLines.drop(1).toArray
  // upstream/master
  var position = 0
  var people=peopleFile.length
  var friends=friendFile.length
  println("Start: "+ LocalDateTime.now())

  protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout => AllocateSpoutTask(Duration(1,NANOSECONDS),"newLine")
    case "newLine" => {
      if (position<=people) sendTuple("person|"+peopleFile(position))
      if (position<=friends) sendTuple("person_knows_person|"+friendFile(position))
      position += 1
      if(position>friends)
        println("ingestion Finished")
      else
        AllocateSpoutTask(Duration(1,NANOSECONDS),"newLine")
    }
    case _ => println("message not recognized!")
  }
}
