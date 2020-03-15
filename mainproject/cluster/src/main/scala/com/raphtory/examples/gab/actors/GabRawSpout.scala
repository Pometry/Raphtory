package com.raphtory.examples.gab.actors

import akka.actor.Cancellable
import ch.qos.logback.classic.Level
import com.mongodb.casbah.Imports.{MongoConnection, _}
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
  private var window = 1000
  private var postMin = 0
  private var postMax = 1001


  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  // private val mongoLogger = Logger.getLogger("org.mongodb.driver.cluster")
  // mongoLogger.setLevel(Level.OFF)

  override def preStart() {
    super.preStart()
    sched = context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, "parsePost")
    context.system.scheduler.scheduleOnce(Duration(1, MINUTES), self, "required") //for setting the amount of messages
  }

  override protected def processChildMessages(rcvdMessage: Any): Unit = {
    rcvdMessage match {
      case "parsePost" => running()
      case "required" => {
        window = System.getenv().getOrDefault("UPDATES_FREQ", "1000").toInt
        postMax+=(window-1000)
        println(s"Warm-up finished, updater now running at ${window} posts/s")}
    }
  }

  def running(): Unit = {
    if (isSafe) {
      val count = getNextPosts()
      postMin += window
      postMax += window
      println(s"Current min post is $postMin, max post is $postMax, last call retrieved $count posts")
      context.system.scheduler.scheduleOnce(Duration(10, MILLISECONDS), self, "parsePost")
    }
    else{
      context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "parsePost")
    }
  }

  private def getNextPosts():Int = {
    var count =0
    for (x <- mongoColl.find("_id" $lt postMax $gt postMin)) {
      try {
        val data = x.get("data").toString.drop(2).dropRight(1).replaceAll("""\\"""", "").replaceAll("""\\""", "")
        count +=1
        //println(data)
        sendCommand(data)
      } catch {
        case e: Throwable =>
          println("Cannot parse record")
      }
    }
    return count
  }

}


//redis-server --dir /home/moe/ben/gab --dbfilename gab.rdb --daemonize yes