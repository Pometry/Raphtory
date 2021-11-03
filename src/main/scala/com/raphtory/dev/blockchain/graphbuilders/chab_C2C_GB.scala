package com.raphtory.dev.blockchain.graphbuilders

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.{FloatProperty, LongProperty, Properties, StringProperty, Type}

class chab_C2C_GB extends GraphBuilder[String]{

  override def parseTuple(tuple: String) = {
    try{
      val dp = formatLine(tuple.split(",").map(_.trim))
      val transactionTime = dp.time
      val srcClusterId = dp.srcCluster
      val dstClusterId = dp.dstCluster
      val transactionId = dp.txid
      val btcAmount = dp.amount
      val usdAmount = dp.usd

      addVertex(transactionTime, srcClusterId, Properties(StringProperty("ID", dp.srcCluster.toString)), Type("Cluster"))
      addVertex(transactionTime, dstClusterId, Properties(StringProperty("ID", dp.dstCluster.toString)), Type("Cluster"))

      addEdge(transactionTime,
        srcClusterId,
        dstClusterId,
        Properties(
          FloatProperty("BitCoin", btcAmount),
          FloatProperty("USD", usdAmount),
          LongProperty("Transaction", transactionId)
        ),
        Type("Transfer")
      )
  }catch { case e: Exception => println(e, tuple) }
  }

  //converts the line into a case class which has all of the data via the correct name and type
  def formatLine(line: Array[String]): Datapoint =
    Datapoint(
      line(1).toFloat / 100000000, 			//Amount of transaction in BTC
      line(2).toLong, 			            //ID of destination cluster
      line(3).toLong, 			            //ID of source cluster
      line(4).toLong * 1000,            //Time of transaction in seconds (milli in Raph)
      line(5).toLong, 			            //ID of transaction, can be similar for many records
      line(6).toFloat / 100000 			    //Amount of transaction in USD

    )

  def longCheck(data: String): Option[Long] = if (data equals "") None else Some(data.toLong)


  case class Datapoint(
                        amount: Float,       //Amount of transaction in Satoshi
                        dstCluster: Long,   //ID of destination cluster
                        srcCluster: Long,   //ID of source cluster
                        time: Long,         //Time of transaction in seconds
                        txid: Long,          //ID of transaction, can be similar for many records
                        usd: Float          //Amount of transaction in USD
                      )
}
