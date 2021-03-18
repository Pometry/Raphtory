package com.raphtory.dev.networkx

import java.time.LocalDateTime

import com.raphtory.core.actors.Spout.Spout

import scala.collection.mutable
import scala.language.postfixOps

class networkxSpout extends Spout[String] {

    println("Start: " + LocalDateTime.now())
    val directory = System.getenv().getOrDefault("FILE_SPOUT_DIRECTORY", "/app").trim
    val fileName = System.getenv().getOrDefault("FILE_SPOUT_FILENAME", "").trim //
    val filename = directory + "/" + fileName
    val fileQueue = mutable.Queue[String]()

    override def setupDataSource(): Unit = {
      val fn = scala.io.Source.fromFile(filename)
      fileQueue ++= fn.getLines
      fn.close()
    }

    override def generateData(): Option[String] = {
      if(fileQueue isEmpty){
        dataSourceComplete()
        None
      }
      else
        Some(fileQueue.dequeue())
    }
    override def closeDataSource(): Unit = {}
  }