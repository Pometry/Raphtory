package com.raphtory.algorithms

import com.raphtory.core.model.analysis.entityVisitors.VertexVisitor

import scala.collection.parallel.ParMap

/**
A version of MultiLayerLPA that gives freedom over filering the edges to consider
-- Work specific to the Word Semantics Project
 **/

object MultiLayerLPAparams {
  def apply(args: Array[String]): MultiLayerLPAparams = new MultiLayerLPAparams(args)
}
class MultiLayerLPAparams(args: Array[String]) extends MultiLayerLPA(args) {
  //args = [top, weight, maxiter, start, end, layer-size, omega, theta]
  val theta: Double = if (arg.length < 7) 0.0 else args(7).toDouble

  override def weightFunction(v: VertexVisitor, ts: Long): ParMap[Long, Long] =
    (v.getInCEdgesBetween(ts - snapshotSize, ts) ++ v.getOutEdgesBetween(ts - snapshotSize, ts))
      .filter(e => e.getPropertyValueAt("ScaledFreq", ts).getOrElse(1.0).asInstanceOf[Double] > theta)
      .map(e => (e.ID(), e.getPropertyValue(weight).getOrElse(1L).asInstanceOf[Long])) //  im: fix this one after pulling new changes
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.size)
}
