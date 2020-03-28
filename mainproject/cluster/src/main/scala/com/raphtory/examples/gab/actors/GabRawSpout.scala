package com.raphtory.examples.gab.actors

import akka.actor.Cancellable
import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.MongoConnection
import com.mongodb.casbah.Imports._
import com.raphtory.core.components.Spout.SpoutTrait
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

final class GabRawSpout extends SpoutTrait {

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
  // private val mongoLogger = Logger.getLogger("org.mongodb.driver.cluster")
  // mongoLogger.setLevel(Level.OFF)

  override protected def ProcessSpoutTask(message: Any): Unit = message match {
    case StartSpout  => AllocateSpoutTask(Duration(1, MILLISECONDS), "parsePost")
    case "parsePost" => running()
  }

  def running(): Unit = {
    val count = getNextPosts()
    postMin += window
    postMax += window
    println(s"Current min post is $postMin, max post is $postMax, last call retrieved $count posts")
    AllocateSpoutTask(Duration(10, MILLISECONDS), "parsePost")

  }

  private def getNextPosts(): Int = {
    var count = 0
    for (x <- mongoColl.find("_id" $lt postMax $gt postMin))
      try {
        val data = x.get("data").toString.drop(2).dropRight(1).replaceAll("""\\"""", "").replaceAll("""\\""", "")
        count += 1
        //println(data)
        sendTuple(data)
      } catch {
        case e: Throwable =>
          println("Cannot parse record")
      }
    return count
  }

}
//redis-server --dir /home/moe/ben/gab --dbfilename gab.rdb --daemonize yes
