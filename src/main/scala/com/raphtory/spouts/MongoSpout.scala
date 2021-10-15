package com.raphtory.spouts


import akka.actor.Cancellable
import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.{MongoConnection, _}
import com.raphtory.core.components.spout.Spout
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.postfixOps

class MongoSpout(IP:String, port:Int, database:String, collection:String ) extends Spout[String] {

  private var sched: Cancellable = null

  private val mongoConn = MongoConnection(IP, port)
  private val mongoColl = mongoConn(database)(collection)
  private var window    = 1000
  private var postMin   = 0
  private var postMax   = 1001

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)

  var queue = mutable.Queue[Option[String]]()

  override def generateData(): Option[String] = {
    if(queue.isEmpty)
      getNextPosts()
    queue.dequeue()

  }

  private def getNextPosts() = {

    for (x <- mongoColl.find("_id" $lt postMax $gt postMin))
      try {
        val data = x.get("data").toString.drop(2).dropRight(1).replaceAll("""\\"""", "").replaceAll("""\\""", "")
        queue += Some(data)
      } catch {
        case e: Throwable =>
          println("Cannot parse record")
      }
    postMin += window
    postMax += window
  }

  override def setupDataSource(): Unit = {}
  override def closeDataSource(): Unit = {}
}
