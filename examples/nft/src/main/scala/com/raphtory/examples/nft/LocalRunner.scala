package com.raphtory.examples.nft

import com.raphtory.RaphtoryApp
import com.raphtory.api.input.Graph.assignID
import com.raphtory.api.input.MutableDouble
import com.raphtory.api.input.Graph
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.Properties
import com.raphtory.api.input.MutableString
import com.raphtory.api.input.Type
import com.raphtory.examples.nft.analysis.CycleMania
import com.raphtory.formats.JsonFormat
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.FileSink
import com.raphtory.utils.FileUtils

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.collection.mutable

object LocalRunner extends RaphtoryApp.Local {

  val path = "/tmp/Data_API_reduced.csv"
  val url  = "https://osf.io/download/kaumt/"
  FileUtils.curlFile(path, url)

  val eth_historic_csv = "/tmp/ETH-USD.csv"
  val url_eth          = "https://osf.io/download/mw3vh/"
  FileUtils.curlFile(eth_historic_csv, url_eth)

  def setupDatePrices(eth_historic_csv: String): mutable.HashMap[String, Double] = {
    val src            = scala.io.Source.fromFile(eth_historic_csv)
    val date_price_map = new mutable.HashMap[String, Double]()
    src.getLines.drop(1).foreach { line =>
      val l = line.split(",").toList
      date_price_map.put(l(0), (l(1).toDouble + l(2).toDouble) / 2)
    }
    src.close()
    date_price_map
  }

  var date_price = setupDatePrices(eth_historic_csv = eth_historic_csv)

  def addToGraph(graph: Graph, tuple: String): Unit = {
    val fileLine            = tuple.split(",").map(_.trim)
    // Skip Header
    if (fileLine(0) == "Smart_contract") return
    // Seller details
    val seller_address      = fileLine(3)
    val seller_address_hash = assignID(seller_address)
    // Buyer details
    val buyer_address       = fileLine(5)
    val buyer_address_hash  = assignID(buyer_address)
    // Transaction details
    val datetime_str        = fileLine(13)
    val timeStamp           = LocalDateTime
      .parse(datetime_str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      .toEpochSecond(ZoneOffset.UTC)
    val tx_hash             = fileLine(2)
    val token_id_str        = fileLine(1)
    val token_id_hash       = assignID(token_id_str)
    val crypto              = fileLine(8)
    if (crypto != "ETH")
      return
    var price_USD           = 0.0
    if (fileLine(9) == "")
      price_USD = date_price(datetime_str.substring(0, 10))
    else
      price_USD = fileLine(9).toDouble

    // NFT Details
    val collection_cleaned = fileLine(14)
    val market             = fileLine(11)
    val category           = fileLine(15)

    // add buyer node
    graph.addVertex(
            timeStamp,
            buyer_address_hash,
            Properties(ImmutableString("address", buyer_address)),
            Type("Wallet")
    )

    // Add node for NFT
    graph.addVertex(
            timeStamp,
            token_id_hash,
            Properties(
                    ImmutableString("id", token_id_str),
                    ImmutableString("collection", collection_cleaned),
                    ImmutableString("category", category)
            ),
            Type("NFT")
    )

    // Creating a bipartite graph,
    // add edge between buyer and nft
    graph.addEdge(
            timeStamp,
            buyer_address_hash,
            token_id_hash,
            Properties(
                    MutableString("transaction_hash", tx_hash),
                    MutableString("crypto", crypto),
                    MutableDouble("price_USD", price_USD),
                    MutableString("market", market),
                    MutableString("token_id", token_id_str),
                    MutableString("buyer_address", buyer_address)
            ),
            Type("Purchase")
    )
  }

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val file   = scala.io.Source.fromFile(path)
      file.getLines.foreach(line => addToGraph(graph, line))
      val atTime = 1561661534

      graph
        .at(atTime)
        .past()
        .execute(CycleMania())
        .writeTo(FileSink("/tmp/raphtory_nft_scala", format = JsonFormat()))
        .waitForJob()
    }
}
