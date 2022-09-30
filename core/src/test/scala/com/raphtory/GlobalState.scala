package com.raphtory

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.communication.SchemaProviderInstances._

/**
  * Simple algorithm which takes gets the vertices to send their neighbours their name.
  *  The names of all vertices neighbours are then combined into a string and the length taken.
  *  Finally this length is added into several types of accumulators to check that the correct result is garnered for all of them.
  *  This also tests the GraphFunctionCompleteWithState within the Query Handler
  *  TODO add in tests for accumulators of different types - test default values and not refreshing the value on a new superstep
  */

class GlobalState extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { graphState: graphstate.GraphState =>
        graphState.newMax[Int]("name length max")
        graphState.newMin[Int]("name length min")
        graphState.newAdder[Int]("name length total")
        graphState.newMultiplier[Long](
                "name length multiplier"
        ) //Notable that this really needs to be a long, otherwise it just returns zero
      }
      .step { (vertex, graphState) =>
        vertex.messageInNeighbours(vertex.name())
      }
      .step { (vertex, graphState) =>
        val totalNameLength = vertex.messageQueue[String].fold("")(_ + _).length
        graphState("name length max") += totalNameLength
        graphState("name length min") += totalNameLength
        graphState("name length total") += totalNameLength
        if (totalNameLength != 0) //so that the multiplied value doesn't just end up as 0
          graphState("name length multiplier") += totalNameLength.toLong
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(graphState =>
      Row(
              graphState("name length max").value,
              graphState("name length min").value,
              graphState("name length total").value,
              graphState("name length multiplier").value
      )
    )
}
