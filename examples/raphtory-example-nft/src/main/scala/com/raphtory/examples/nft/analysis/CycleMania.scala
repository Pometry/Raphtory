package com.raphtory.examples.nft.analysis

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

import scala.collection.mutable
// lets get ready to rumble

class CycleMania(moneyCycles: Boolean = true) extends Generic {

  //  def printCycleInfo(purchasers: List[Any], i: Int, j: Int): Unit = {
  //    print("Found an NFT cycle that sold for profit : ")
  //    for (k <- i to j) {
  //      print(" "+purchasers(k))
  //    }
  //    println()
  //  }

  final val HAS_CYCLE: String    = "HAS_CYCLE"
  final val CYCLES_FOUND: String = "CYCLES_FOUND"

  // FASTER ALGORITHM
  override def apply(graph: GraphPerspective): graph.Graph =
    graph
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
                  if (previousPrice < currentPrice) {
                    // println(f"Money Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                    allCyclesFound = Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
                  }
                }
                else {
                  // println(f"All Cycle found, item $buyerId, from ${buyersSeen.get(buyerId)} to $position ")
                  buyersSeen.update(buyerId, position)
                  allCyclesFound = Cycle(purchasers.slice(previousBuyerPosition, position + 1).toArray[Sale]) :: allCyclesFound
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

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .explodeSelect { vertex =>
        val vertexType          = vertex.Type()
        val has_cycle: Boolean = vertex.getStateOrElse(HAS_CYCLE, false)
        if (vertexType == "NFT" & has_cycle) {
          val nftID                   = vertex.getPropertyOrElse("id", "_UNKNOWN_")
          val cyclesFound: List[Cycle] = vertex.getState(CYCLES_FOUND)
          val nftCollection = vertex.getPropertyOrElse("collection", "_UNKNOWN_")
          val nftCategory = vertex.getPropertyOrElse("category", "_UNKNOWN_")
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

  case class Node(nft_id: String, nft_collection: String, nft_category: String, cycles_found: Int, cycle_data: CycleData)

}

object CycleMania {
  def apply() = new CycleMania()
}

// O(n^2)
//  override def apply(graph: GraphPerspective): graph.Graph =
//    graph
//      .step { vertex =>
//        // only for vertexes that are of type NFT
//        if (vertex.Type() == "NFT") {
//          var allCyclesFound: List[Cycle] = List()
//          // get all of my incoming exploded edges and sort them by time
//          val allPurchases = vertex.explodeInEdges().sortBy(e => e.timestamp)
//          // get all the buyers
//          // SRC = seller, DST = NFT they bought, Price_USD = Price,
//          val purchasers = allPurchases.map(e => Sale(e.src.toString, e.getPropertyOrElse("price_USD", 0.0), e.getPropertyOrElse("transaction_hash", ""), e.dst.toString))
//          if (purchasers.size > 2) {
//            //            println(purchasers)
//            // for each buyer, and when they bought it print this result, O(n^2) bad complexity
//            // check if all the nfts are the same, if not then skip
//            for ((finding_id, i) <- purchasers.view.zipWithIndex) {
//              for ((found_id, j) <- purchasers.view.zipWithIndex.takeRight(purchasers.size - i - 1)) {
//                //                println(purchasers.size, i, j)
//                // if the start and end nodes are the same, AND, the NFT sold for more money then print it
//                if (finding_id.buyer == found_id.buyer) { // The SRC ids, i.e. the Users are the same, so seller bought again
//                  if (finding_id.price_usd < found_id.price_usd) { // and the price before is more than the price after
//                    // found an nft cycle now we save it
//                    allCyclesFound = Cycle(purchasers.slice(i, j + 1)) :: allCyclesFound
//                  }
//                }
//              }
//            }
//          }
//          if (allCyclesFound.size > 0) {
//            vertex.setState(CYCLES_FOUND, allCyclesFound)
//            vertex.setState(HAS_CYCLE, true)
//          }
//        }
//      }
