package com.raphtory.examples.nft.graphbuilder

import com.raphtory.api.input.{DoubleProperty, GraphBuilder, ImmutableProperty, Properties, StringProperty, Type}

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.mutable
import scala.io.Source

class NFTGraphBuilder extends GraphBuilder[String] {

  def setupDatePrices(): mutable.HashMap[String, Double] = {
    val eth_historic_csv ="/tmp/ETH-USD.csv"
    // val eth_historic_csv ="/home/ubuntu/data/ETH-USD.csv"
    val src = Source.fromFile(eth_historic_csv)
    val date_price_map = new mutable.HashMap[String,Double]()
    src.getLines.drop(1).foreach { line =>
      val l = line.split(",").toList
      date_price_map.put(l(0), (l(1).toDouble + l(2).toDouble) / 2)
    }
    src.close()
    date_price_map
  }

  var date_price = setupDatePrices()

  override def parseTuple(tuple: String): Unit = {
    val fileLine = tuple.split(",").map(_.trim)
      // Skip Header
      if (fileLine(0) == "Smart_contract") return
      // Seller details
      val seller_address = fileLine(3)
      val seller_address_hash = assignID(seller_address)
      // Buyer details
      val buyer_address = fileLine(5)
      val buyer_address_hash = assignID(buyer_address)
      // Transaction details
      val datetime_str = fileLine(13)
      val timeStamp = LocalDateTime.parse(datetime_str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC)
      val tx_hash = fileLine(2)
      val token_id_str = fileLine(1)
      val token_id_hash = assignID(token_id_str)
      val crypto = fileLine(8)
      if (crypto != "ETH")
        return
      var price_USD = 0.0
      if (fileLine(9) == "") {
        price_USD = date_price(datetime_str.substring(0, 10))
      } else {
        price_USD = fileLine(9).toDouble
      }

      // NFT Details
      val collection_cleaned = fileLine(14)
      val market = fileLine(11)
      val category = fileLine(15)

      // add buyer node
      addVertex(
        timeStamp,
        buyer_address_hash,
        Properties(ImmutableProperty("address", buyer_address)),
        Type("Wallet")
      )
      // add seller node
      addVertex(
        timeStamp,
        seller_address_hash,
        Properties(ImmutableProperty("address", seller_address)),
        Type("Wallet")
      )

      // Add node for NFT
      addVertex(
        timeStamp,
        token_id_hash,
        Properties(
          ImmutableProperty("id", token_id_str),
          ImmutableProperty("collection", collection_cleaned),
          ImmutableProperty("category", category)
        ),
        Type("NFT")
      )

      // Creating a bipartite graph,
      // add edge between buyer and nft
      addEdge(
        timeStamp,
        buyer_address_hash,
        token_id_hash,
        Properties(
          StringProperty("transaction_hash", tx_hash),
          StringProperty("crypto", crypto),
          DoubleProperty("price_USD", price_USD),
          StringProperty("market", market),
          StringProperty("token_id", token_id_str),
          StringProperty("buyer_address", buyer_address)
        ),
        Type("Purchase")
      )
  }
}

