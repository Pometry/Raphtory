package com.raphtory.examples.nft.analysis

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.collection.mutable
// lets get ready to rumble

class CycleMania(moneyCycles: Boolean = true) extends Generic {

  // helper function to print cycle information
  def printCycleInfo(purchasers: List[Any], i: Int, j: Int): Unit = {
    print("Found an NFT cycle that sold for profit : ")
    for (k <- i to j)
      print(" " + purchasers(k))
    println()
  }

  final val HAS_CYCLE: String    = "HAS_CYCLE"
  final val CYCLES_FOUND: String = "CYCLES_FOUND"

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.reducedView
      .step { vertex =>
        // only for vertexes that are of type NFT
        if (vertex.Type() == "NFT") {
          var allCyclesFound: List[Cycle] = List()
          // get all of my incoming exploded edges and sort them by time
          val allPurchases                = vertex.explodeInEdges().sortBy(e => e.timestamp)
          // get all the buyers
          // SRC = seller, DST = NFT they bought, Price_USD = Price,
          val purchasers                  = allPurchases.map(e =>
            Sale(
                    e.getPropertyOrElse("buyer_address", "_UNKNOWN_"),
                    e.getPropertyOrElse("price_USD", 0.0),
                    e.timestamp,
                    e.getPropertyOrElse("transaction_hash", ""),
                    e.getPropertyOrElse("token_id", "_UNKNOWN_")
            )
          )
          if (purchasers.size > 2) {
            // for each buyer, and when they bought it print this as a cycle
            // we keep track of buyers we have seen
            val buyersSeen = mutable.HashMap[String, Int]()
            for ((itemSale, position) <- purchasers.view.zipWithIndex) {
              // If we have not seen this buyer, then add it to the hashmap with the index it was seen
              val buyerId = itemSale.buyer
              if (!buyersSeen.contains(buyerId))
                buyersSeen.addOne((buyerId, position))
              // if we have seen this buyer, then it means we have a cycle omg
              // we print this as a cycle we have seen, then we update the position
              // as we do not want to double count the cycles
              else {
                // but only if the buyer has paid for the item more the second time
                val previousBuyerPosition = buyersSeen.getOrElse(buyerId, -1)
                val previousPrice         = purchasers(previousBuyerPosition).price_usd
                val currentPrice          = itemSale.price_usd
                if (moneyCycles) {
                  // ISSUE: If user is the same and at a loss, then the cycle keeps going.
                  buyersSeen.update(buyerId, position)
                  if (previousPrice < currentPrice)
                    // println(f"Money Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                    allCyclesFound =
                      Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
                }
                else {
                  // println(f"All Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                  buyersSeen.update(buyerId, position)
                  allCyclesFound =
                    Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
                }
              }
            }
          }
          if (allCyclesFound.nonEmpty) {
            vertex.setState(CYCLES_FOUND, allCyclesFound)
            vertex.setState(HAS_CYCLE, true)
          }
        }
      }
      .asInstanceOf[graph.Graph]

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val vertexType         = vertex.Type()
        val has_cycle: Boolean = vertex.getStateOrElse(HAS_CYCLE, false)
        if (vertexType == "NFT" & has_cycle) {
          val nftID                    = vertex.getPropertyOrElse("id", "_UNKNOWN_")
          val cyclesFound: List[Cycle] = vertex.getState(CYCLES_FOUND)
          val nftCollection            = vertex.getPropertyOrElse("collection", "_UNKNOWN_")
          val nftCategory              = vertex.getPropertyOrElse("category", "_UNKNOWN_")
          cyclesFound.map { singleCycle =>
            val cycleData: CycleData = CycleData(
                    buyer = singleCycle.sales.head.buyer,
                    profit_usd = singleCycle.sales.last.price_usd - singleCycle.sales.head.price_usd,
                    cycle = singleCycle
            )
            Row(
                    nftID,
                    nftCollection,
                    nftCategory,
                    singleCycle.sales.length,
                    cycleData
            )
          }
        }
        else
          List(Row())
      }
      .filter(row => row.getValues().nonEmpty)

  case class Sale(buyer: String, price_usd: Double, time: Long, tx_hash: String, nft_id: String)

  case class Cycle(sales: Array[Sale])

  case class CycleData(buyer: String, profit_usd: Double, cycle: Cycle)

  case class Node(
      nft_id: String,
      nft_collection: String,
      nft_category: String,
      cycles_found: Int,
      cycle_data: CycleData
  )

}

object CycleMania {
  def apply() = new CycleMania()
}
