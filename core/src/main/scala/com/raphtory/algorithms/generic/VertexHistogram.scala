package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphstate.Histogram
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.utils.Bounded

import scala.reflect.ClassTag

class VertexHistogram[T: Numeric: Bounded: ClassTag](propertyString: String, noBins: Int = 1000) extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph =
    graph
      .setGlobalState { state =>
        state.newMin[T]("propertyMin", retainState = true)
        state.newMax[T]("propertyMax", retainState = true)
      }
      .step { (vertex, state) =>
        state("propertyMin") += vertex.getState(propertyString, includeProperties = true)
        state("propertyMax") += vertex.getState(propertyString, includeProperties = true)
      }
      .setGlobalState { state =>
        val propertyMin: T = state("propertyMin").value
        val propertyMax: T = state("propertyMax").value
        state.newHistogram[T]("propertyDist", noBins = noBins, propertyMin, propertyMax)
      }
      // Populate histogram with weights
      .step { (vertex, state) =>
        val histogram = state("propertyDist")
        histogram += vertex.getState[T](propertyString)
      }

  override def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect { state =>
      val rowSeq = Seq(state[T, T]("propertyMin").value, state[T, T]("propertyMax").value) ++ state[
              T,
              Histogram[T]
      ]("propertyDist").value.getBins.toSeq
      Row(rowSeq: _*)
    }
}

object VertexHistogram {

  def apply[T: Numeric: Bounded: ClassTag](stateString: String, noBins: Int = 1000) =
    new VertexHistogram[T](stateString, noBins)
}
