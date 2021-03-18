package com.raphtory.dev.blockchain.graphbuilders

import com.raphtory.core.actors.Router.GraphBuilder
import com.raphtory.core.model.communication.{Type, _}

class bitcoin_mixers_GB extends GraphBuilder[String] {

  override def parseTuple(tuple: String) =
    try {
      val dp              = formatLine(tuple.split(",").map(_.trim))
      val transactionTime = dp.time
      val srcClusterId    = dp.srcCluster
      val dstClusterId    = dp.dstCluster
      val btcAmount       = dp.amount

      addVertex(transactionTime, srcClusterId, Type(dp.srcType))
      addVertex(transactionTime, dstClusterId, Type(dp.dstType))

      addEdge(
        transactionTime,
        srcClusterId,
        dstClusterId,
        Properties(DoubleProperty("BitCoin", btcAmount)),
        Type("Transfer")
      )
    } catch { case e: Exception => println(e, tuple) }

  //converts the line into a case class which has all of the data via the correct name and type
  def formatLine(line: Array[String]): Datapoint =
    Datapoint(
            line(5).toDouble / 100000000, //Amount of transaction in BTC
            line(3).toLong,               //ID of destination cluster
            line(1).toLong,               //ID of source cluster
            line(0).toLong * 1000,        //Time of transaction in seconds (milli in Raph)
            if (line(2) == "tx") "Transaction" else "Addr",
            if (line(4) == "tx") "Transaction" else "Addr"
    )

  def longCheck(data: String): Option[Long] = if (data equals "") None else Some(data.toLong)

  case class Datapoint(
      amount: Double,   //Amount of transaction in Satoshi
      dstCluster: Long, //ID of destination cluster
      srcCluster: Long, //ID of source cluster
      time: Long,       //Time of transaction in seconds
      srcType: String,
      dstType: String
  )

}
