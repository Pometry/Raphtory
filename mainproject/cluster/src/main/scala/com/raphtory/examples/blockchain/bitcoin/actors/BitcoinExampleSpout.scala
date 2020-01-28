package com.raphtory.examples.blockchain.bitcoin.actors

import akka.actor._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse, SpoutGoing}
import com.raphtory.examples.blockchain.BitcoinTransaction

import kamon.Kamon
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scalaj.http.{Http, HttpRequest}

class BitcoinExampleSpout extends SpoutTrait {

  var blockcount = 1
  var folder =  System.getenv().getOrDefault("BITCOIN_DIRECTORY", "/app/blocks/").trim
  override def preStart() { //set up partition to report how many messages it has processed in the last X seconds
    super.preStart()
    context.system.scheduler.schedule(Duration(1, SECONDS), Duration(1, MILLISECONDS), self, "parseBlock")

  }

  //************* MESSAGE HANDLING BLOCK
  override def processChildMessages(message:Any): Unit = {
    message match {
      case "parseBlock" => running()
      case _ => println("message not recognized!")
    }
  }

  def running() : Unit = if(isSafe()) {
    getTransactions()
    blockcount +=1
  }

  def getTransactions():Unit = {
    if(blockcount>20000) return
    val result = readNextBlock().parseJson.asJsObject
    val time = result.fields("time")
    val blockID = result.fields("hash")
    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){

      sendCommand(BitcoinTransaction(time,blockcount,blockID,transaction))
      //val time = transaction.asJsObject.fields("time")

    }
  }

  def readNextBlock():String={
    //val bufferedSource = Source.fromFile(s"/Users/lagordamotoneta/Documents/QMUL/QMUL/project/Datasets/blocks/$blockcount.txt")

    val bufferedSource = Source.fromFile(s"$folder/$blockcount.txt")

    var block =""
    for (line <- bufferedSource.getLines) {
      block +=line
    }
    bufferedSource.close
    blockcount+=1
    block
  }

}
