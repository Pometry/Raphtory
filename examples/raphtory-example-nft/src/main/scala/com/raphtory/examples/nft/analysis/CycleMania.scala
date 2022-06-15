package com.raphtory.examples.nft.analysis

import com.raphtory.algorithms.api.{GraphAlgorithm, GraphPerspective}

// lets get ready to rumble
class CycleMania(name: String = "Gandalf") extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .step { vertex =>
        // only for vertexes that are of type NFT
        if (vertex.Type() == "NFT") {
          // get all of my incoing exploded edges and sort them by time
          val allPurchases = vertex.explodeInEdges().sortBy(e => e.timestamp)
          // get all the buyers
          val purchasers = allPurchases.map(e => e.src())
          // for each buyer, and when they bought it print this result
          for ((finding_id, i) <- purchasers.view.zipWithIndex) {
            for ((found_id, j) <- purchasers.view.zipWithIndex.takeRight(purchasers.size - i - 1)) {
              if (finding_id == found_id) {
                if (finding_id ==  1401007316307435307L ){
                  val x=1
                }
                print("Found a cycle ")
                print(i, finding_id, j, found_id)
                println()
              }
            }
          }

      }
  }
}

object CycleMania {
  def apply() = new CycleMania()
}