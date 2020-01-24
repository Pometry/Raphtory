package com.raphtory.examples.blockchain.bitcoin.analysis

import com.raphtory.core.analysis.API.{Analyser, WorkerID}
import com.raphtory.examples.blockchain.CoinsAquiredPayload

import scala.collection.mutable.ArrayBuffer

class BitcoinAnalyser extends Analyser {

  //(implicit proxy: GraphRepoProxy.type, managerCount: Int,workerID:Int):
  override def analyse(): Unit = {
    var results = ArrayBuffer[(String, Double)]()
    var currentBlock = 0
    var hash = ""
    proxy.getVerticesSet().foreach(v => {
      val vertex = proxy.getVertex(v._2)
      val vertexType = vertex.getPropertyCurrentValue("type").getOrElse("no-type")
      if(vertexType.equals("address")) {
        val address = vertex.getPropertyCurrentValue("address").getOrElse("no address")
        var total: Double = 0
        for ((id,edge) <- vertex.getIngoingNeighbors) {
          val edgeValue = vertex.getIngoingNeighborProp(id, "value").getOrElse("0")
          total += edgeValue.toDouble
        }
        results :+= (address, total)
      }
      else if(vertexType.equals("transaction")){
        val block = vertex.getPropertyCurrentValue("block").getOrElse("0")
        if(block.toInt>currentBlock){
          currentBlock=block.toInt
          hash= vertex.getPropertyCurrentValue("blockhash").getOrElse("0")
        }
      }
    })
    //println("Sending step end")

    CoinsAquiredPayload(WorkerID(workerID),results.sortBy(f => f._2)(Ordering[Double].reverse).take(10),currentBlock,hash)
  }



  override def setup(): Unit = {

  }

  override def defineMaxSteps(): Int = 1

  override def processResults(results: ArrayBuffer[Any], timeStamp: Long, viewCompleteTime: Long): Unit = {
    var finalResults = ArrayBuffer[(String, Double)]()
    var highestBlock = 0
    var blockHash = ""
    for(indiResult <- results.asInstanceOf[(ArrayBuffer[CoinsAquiredPayload])]){
      for (pair <- indiResult.wallets){
        finalResults :+= pair
      }
      if(indiResult.highestBlock>highestBlock){
        highestBlock = indiResult.highestBlock
        blockHash = indiResult.blockhash
      }
    }
    println(s"Current top three wallets at block $highestBlock ($blockHash)")
    finalResults.sortBy(f => f._2)(Ordering[Double].reverse).take(3).foreach(pair =>{
      println(s"${pair._1} has acquired a total of ${pair._2} bitcoins ")
    })

  }

  override def processViewResults(results: ArrayBuffer[Any], timestamp: Long, viewCompleteTime: Long): Unit = {}

  override def processWindowResults(results: ArrayBuffer[Any], timestamp: Long, windowSize: Long, viewCompleteTime: Long): Unit = {}

  override def returnResults(): Any = ???
}



