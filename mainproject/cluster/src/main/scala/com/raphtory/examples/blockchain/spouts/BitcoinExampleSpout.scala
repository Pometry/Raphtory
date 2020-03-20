package com.raphtory.examples.blockchain.spouts

import com.raphtory.core.components.Spout.SpoutTrait
import com.raphtory.examples.blockchain.BitcoinTransaction
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class BitcoinExampleSpout extends SpoutTrait {

  var blockcount = 1
  var folder =  System.getenv().getOrDefault("BITCOIN_DIRECTORY", "/app/blocks/").trim

  //************* MESSAGE HANDLING BLOCK
  override def ProcessSpoutTask(message:Any): Unit = message match {
      case StartSpout => AllocateSpoutTask(Duration(1,MILLISECONDS),"parseBlock")
      case "parseBlock" => {
        getTransactions()
        blockcount +=1
      }
      case _ => println("message not recognized!")
    }


  def getTransactions():Unit = {
    if(blockcount>20000) return
    val result = readNextBlock().parseJson.asJsObject
    val time = result.fields("time")
    val blockID = result.fields("hash")
    for(transaction <- result.asJsObject().fields("tx").asInstanceOf[JsArray].elements){

      sendTuple(BitcoinTransaction(time,blockcount,blockID,transaction))
      //val time = transaction.asJsObject.fields("time")

    }
    AllocateSpoutTask(Duration(1,MILLISECONDS),"parseBlock")
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
