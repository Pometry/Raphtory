package com.raphtory.examples.TestPackage

import java.text.SimpleDateFormat

import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.utils.Utils
import kamon.Kamon

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class TestSpout extends SpoutTrait {



  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    context.system.scheduler.schedule(Duration(10, SECONDS), Duration(1, MILLISECONDS), self, "newLine")
  }


  protected def processChildMessages(rcvdMessage: Any): Unit = {
    rcvdMessage match {
      case "NewLine" => {
        if (isSafe()) {
          sendCommand("data goes here")
        }
      }
      case "stop" => stop()
      case _ => println("message not recognized!")
    }
  }

    def running(): Unit = {
      //genRandomCommands(totalCount)
      //totalCount+=1000
    }

  }
