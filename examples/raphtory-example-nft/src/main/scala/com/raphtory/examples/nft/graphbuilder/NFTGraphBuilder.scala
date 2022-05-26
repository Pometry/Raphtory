package com.raphtory.examples.nft.graphbuilder

import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.graphbuilder.Properties.{FloatProperty, ImmutableProperty, LongProperty, Properties, StringProperty, Type}

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

class NFTGraphBuilder extends GraphBuilder[String] {

  override def parseTuple(tuple: String): Unit = {
    val fileLine   = tuple.split(",").map(_.trim)
    // Skip Header
    if (fileLine(0) == "Smart_contract") return
    // Seller details
    val seller_address  =  fileLine(3)
    val seller_address_id = assignID(seller_address)
    // Buyer details
    val buyer_address  =  fileLine(5)
    val buyer_address_id = assignID(buyer_address)
    // Transaction details
    val datetime_str = fileLine(19)
    val timeStamp = LocalDateTime.parse(datetime_str, DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC)
    val tx_hash    = fileLine(2)
    val token_id_str = fileLine(1)
    val token_id = token_id_str.toFloat.toLong
    val crypto = fileLine(12)
    val price_USD      =  fileLine(13).toFloat

    // NFT Details
    val collection_cleaned =  fileLine(22)
    val market =  fileLine(17)
    val category = fileLine(23)

    // add buyer node
    addVertex(
      timeStamp,
      buyer_address_id,
      Properties(ImmutableProperty("address", buyer_address)),
      Type("Wallet")
    )
    // add seller node
    addVertex(
      timeStamp,
      seller_address_id,
      Properties(ImmutableProperty("address", seller_address)),
      Type("Wallet")
    )

    // Add node for NFT
    addVertex(
      timeStamp,
      token_id,
      Properties(
        ImmutableProperty("id", token_id_str),
        ImmutableProperty("collection", collection_cleaned),
        ImmutableProperty("category", category)
      ),
      Type("NFT")
    )

    // Creating a bipitide graph,
    // add edge between buyer and nft
    addEdge(
      timeStamp,
      buyer_address_id,
      token_id,
      Properties(
        ImmutableProperty("transaction_hash", tx_hash),
        ImmutableProperty("crypto", crypto),
        FloatProperty("price_USD", price_USD),
        StringProperty("market", market)
      ),
      Type("Purchase")
    )
  }

}
