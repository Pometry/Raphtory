package com.raphtory.examples.gab.datasources

import akka.actor.Cancellable
import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.{MongoConnection, _}
import com.raphtory.core.actors.Spout.Spout
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.language.postfixOps

final class GabMongoSpout extends Spout[String] {

  //private val redis    = new RedisClient("moe", 6379)
  //private val redisKey = "gab-posts"
  private var sched: Cancellable = null

  //val options: MongoClientOptions = MongoClientOptions.builder.addCommandListener(new LoggingClusterListener).build()
  //ddClusterListener(new LoggingClusterListener).build
  private val mongoConn = MongoConnection("138.37.32.67", 27017)
  private val mongoColl = mongoConn("gab")("posts")
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
//redis-server --dir /home/moe/ben/gab --dbfilename gab.rdb --daemonize yes
