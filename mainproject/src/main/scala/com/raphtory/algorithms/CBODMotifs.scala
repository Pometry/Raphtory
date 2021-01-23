package com.raphtory.algorithms

class CBODMotifs(args:Array[String]) extends CommunityOutlierDetection(args) {
  val omega: Array[Double] = if (args.length>2) args.drop(2).map(_.toDouble) else Array(0.5, 0.3, 0.2) // TODO: rework how to introduce args here
  override def returnResults(): Any = {
    view.getVertices().map { vertex =>
      val obj = new MotifCounting(args)
      val inc  = vertex.getIncEdges
      val outc = vertex.getOutEdges
      val a = (List(vertex.getState[Double]("outlierscore"),
        obj.motifCounting(1, inc, outc),
        obj.motifCounting(2, inc, outc)) zip omega).map(x=> x._1 *x._2).sum
      (vertex.ID(), a)
    }
  }
}