package com.raphtory.examples.blockchain.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication.{Type, _}

class ChABClus2ClusGraphBuilderNoprop extends GraphBuilder[String] {

  override def parseTuple(tuple: String) = {
      val dp = formatLine(tuple.split(",").map(_.trim))
      val transactionTime = dp.time
      val srcClusterId = dp.srcCluster
      val dstClusterId = dp.dstCluster
      sendUpdate(VertexAdd(msgTime = transactionTime, srcID = srcClusterId, Type("Cluster")))
      sendUpdate(VertexAdd(msgTime = transactionTime, srcID = dstClusterId, Type("Cluster")))
      sendUpdate(EdgeAdd(msgTime = transactionTime, srcID = srcClusterId, dstID = dstClusterId, Type("Transfer")))
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
