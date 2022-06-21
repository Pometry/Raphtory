package com.raphtory.algorithms.generic.motif

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{Row, Table}

object GlobalClusteringCoefficient extends Generic{

  override def apply(graph: GraphPerspective): graph.Graph =
    GlobalTriangleCount(graph)
      .setGlobalState{state =>
        state.newAdder[Int]("wedges",retainState = true)
        state.newAdder[Double]("totalClustering",0.0,retainState = true)
      }
      .step{
        (vertex, state) =>
          val k = vertex.degree
          state("totalClustering") += (if (k>1) 2.0*vertex.getState[Int]("triangleCount").toDouble/(k*(k-1)) else 0.0)
          state("wedges")+= k*(k-1)/2
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect{
      state =>
        val totalCluster : Double = state("totalClustering").value
        // the below is thrice the actual number of triangles, hence none of the usual factor of 3 in the
        // global clustering coefficient calc.
        val totalTriangles : Int = state("triangles").value
        val totalWedges : Int = state("wedges").value
        val avgCluster = if (state.nodeCount>0) totalCluster/state.nodeCount else 0.0
        val globalCluster = if (totalWedges > 0) totalTriangles/ totalWedges else 0.0
        Row(avgCluster,globalCluster)
    }

}
