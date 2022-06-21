package com.raphtory.algorithms.generic.motif

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

object GlobalTriangleCount extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    LocalTriangleCount(graph)
      .setGlobalState(state => state.newAdder[Int]("triangles",retainState = true))
      .step { (vertex,state) =>
        val tri = vertex.getState[Int]("triangleCount")
        state("triangles")+= tri
      }

  override def tabularise(graph: GraphPerspective) : Table =
    graph.globalSelect{ state =>
      val totalTri: Int = state("triangles").value
      Row(totalTri/3)
    }
}
