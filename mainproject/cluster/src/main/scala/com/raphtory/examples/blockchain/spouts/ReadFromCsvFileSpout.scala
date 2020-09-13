//package com.raphtory.examples.blockchain.spouts
//
//import com.raphtory.core.components.Spout.SpoutTrait
//
//import scala.io.Source
//
//class ReadFromCsvFileSpout extends SpoutTrait {
//
//  var currentBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_START_BLOCK_INDEX", "9014194").trim.toInt
//  var highestBlock = System.getenv().getOrDefault("SPOUT_ETHEREUM_MAXIMUM_BLOCK_INDEX", "999999999").trim.toInt
//  val nodeIP       = System.getenv().getOrDefault("SPOUT_ETHEREUM_IP_ADDRESS", "127.0.0.1").trim
//  val nodePort     = System.getenv().getOrDefault("SPOUT_ETHEREUM_PORT", "8545").trim
//
//  val filename = "Macintosh HD/Users/imasgo/downloads/transactions9000000_9100000.csv"
//  for (line <- Source.fromFile(filename).getLines) {
//    println(line)
//  }
//
//  override protected def ProcessSpoutTask(message: Any): Unit = message match {
//    case StartSpout  => pullNextBlock()
//    case "nextBlock" => pullNextBlock()
//  }
//
//
//
////  read csv
////  for each line in the csv DO SOMETHING (what???)
//
//
//
//}
