package com.raphtory.algorithms

import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphPerspective, Row, Table}

/**
  Description:
    This algorithm returns the weight of a node, defined by the weighted sum of incoming, outgoing or total edges to that node.
    If an edge has a numerical weight property, the name of this property can be specified as a parameter -- default is "weight".
    In this case, the sum of edge weights for respectively incoming and outgoing edges respectively is returned, as well as the total
    of these. Otherwise, the number of incoming and outgoing edges (including multiple edges between the same node pairs) is returned,
    as well as the sum of these.

  Parameters:
    weightProperty (String) : the property (if any) containing a numerical weight value for each edge, defaults to "weight".
    output (String) : destination of the output file, defaults to "/tmp/WeightedDegree"

  Returns:
    inWeight : Sum of weighted incoming edges
    outWeight : Sum of weighted outgoing edges
    totWeight : Sum of the above
  */

class WeightedDegree(weightProperty:String="weight",output:String = "/tmp/WeightedDegree") extends GraphAlgorithm{

  override def tabularise(graph: GraphPerspective): Table = {
    graph.select({
      vertex =>
        val inWeight = vertex.getInEdges()
          .map(e => e.getPropertyOrElse(weightProperty, 1.0))
          .sum
        val outWeight = vertex.getOutEdges()
          .map(e => e.getPropertyOrElse(weightProperty, 1.0))
          .sum
        val totWeight = inWeight + outWeight
        Row(vertex.getPropertyOrElse("name", vertex.ID()), inWeight, outWeight, totWeight)
    })
  }

  override def write(table: Table): Unit = {
    table.writeTo(output)
  }

}

object WeightedDegree {
  def apply(weightProperty:String="weight",output:String="/tmp/WeightedDegree") = new WeightedDegree(weightProperty,output)
}
