package com.raphtory.examples.blockchain.routers

import java.text.SimpleDateFormat

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._

class ChainalysisABRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val dp = formatLine(record.asInstanceOf[String].split(",").map(_.trim))
    val transactionTime = dp.time
    val srcClusterId = dp.srcCluster
    val dstClusterId = dp.dstCluster
    val transactionId = dp.txid
    val btcAmount = dp.amount
    val usdAmount = dp.usd

    sendGraphUpdate(VertexAdd(transactionTime, srcClusterId, Type("Cluster")))
    sendGraphUpdate(VertexAdd(transactionTime, dstClusterId, Type("Cluster")))
    sendGraphUpdate(VertexAdd(transactionTime, transactionId, Type("Transaction")))

    sendGraphUpdate(
      EdgeAddWithProperties(transactionTime,
        srcClusterId,
        transactionId,
        Properties(DoubleProperty("BitCoin", btcAmount), DoubleProperty("USD",usdAmount)),
        Type("Incoming Payment")
      )
    )
    sendGraphUpdate(
      EdgeAddWithProperties(transactionTime,
        transactionId,
        dstClusterId,
        Properties(DoubleProperty("BitCoin", btcAmount), DoubleProperty("USD",usdAmount)),
        Type("Outgoing Payment")
      )
    )

  }

  //converts the line into a case class which has all of the data via the correct name and type
  def formatLine(line: Array[String]): Datapoint =
    Datapoint(
      line(0).toLong * 1000,            //Time of transaction in seconds (milli in Raph)
      line(1).toLong / 100000000, 			//Amount of transaction in BTC
      line(2).toLong / 100000, 			    //Amount of transaction in USD
      line(3).toLong, 			            //ID of source cluster
      line(4).toLong, 			            //ID of destination cluster
      line(5).toLong 			              //ID of transaction, can be similar for many records
    )

  def longCheck(data: String): Option[Long] = if (data equals "") None else Some(data.toLong)


  case class Datapoint(
                        time: Long,         //Time of transaction in seconds
                        amount: Long,       //Amount of transaction in Satoshi
                        usd: Long,          //Amount of transaction in USD
                        srcCluster: Long,   //ID of source cluster
                        dstCluster: Long,   //ID of destination cluster
                        txid: Long          //ID of transaction, can be similar for many records

                      )
}
