package com.raphtory.examples.blockchain.routers

import java.text.SimpleDateFormat

import java.util.Calendar


import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.Type
import com.raphtory.core.model.communication._

class ChABClus2ClusRouter(override val routerId: Int,override val workerID:Int, override val initialManagerCount: Int) extends RouterWorker[StringSpoutGoing](routerId,workerID, initialManagerCount) {

  def parseTuple(record: Any): Unit = {
      val dp = formatLine(record.asInstanceOf[String].split(",").map(_.trim))
      val transactionTime = dp.time
      val srcClusterId = dp.srcCluster
      val dstClusterId = dp.dstCluster
      val transactionId = dp.txid
      val btcAmount = dp.amount
      val usdAmount = dp.usd

      sendGraphUpdate(VertexAdd(msgTime = transactionTime, srcID = srcClusterId, Type("Cluster")))
      sendGraphUpdate(VertexAdd(msgTime = transactionTime, srcID = dstClusterId, Type("Cluster")))

      sendGraphUpdate(
        EdgeAddWithProperties(msgTime = transactionTime,
          srcID = srcClusterId,
          dstID = dstClusterId,
          Properties(DoubleProperty("BitCoin", btcAmount),
            DoubleProperty("USD", usdAmount),
            DoubleProperty("Transaction", transactionId)),
          Type("Transfer")
        )
      )
  }

  //converts the line into a case class which has all of the data via the correct name and type
  def formatLine(line: Array[String]): Datapoint =
    Datapoint(
      line(1).toDouble / 100000000, 			//Amount of transaction in BTC
      line(2).toLong, 			            //ID of destination cluster
      line(3).toLong, 			            //ID of source cluster
      line(4).toLong * 1000,            //Time of transaction in seconds (milli in Raph)
      line(5).toLong, 			            //ID of transaction, can be similar for many records
      line(6).toDouble / 100000 			    //Amount of transaction in USD

    )

  def longCheck(data: String): Option[Long] = if (data equals "") None else Some(data.toLong)


  case class Datapoint(
                        amount: Double,       //Amount of transaction in Satoshi
                        dstCluster: Long,   //ID of destination cluster
                        srcCluster: Long,   //ID of source cluster
                        time: Long,         //Time of transaction in seconds
                        txid: Long,          //ID of transaction, can be similar for many records
                        usd: Double          //Amount of transaction in USD
                      )
}
