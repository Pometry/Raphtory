package com.raphtory.core.graph.visitor

import com.raphtory.core.algorithm.{EdgeMergeStrategy, MergeStrategy}
import com.raphtory.core.algorithm.MergeStrategy.Merge

trait Edge extends EntityVisitor {

  //information about the edge meta data
  def ID(): Long
  def src(): Long
  def dst(): Long
  def explode(): List[ExplodedEdge]

  def totalWeight(strategy: Merge = MergeStrategy.Sum, weightProperty:String="weight", default:Float=1.0f): Float = {
    strategy match {
      case MergeStrategy.Sum =>
        EdgeMergeStrategy.sumMerge(this,weightProperty,default)
      case MergeStrategy.Max =>
        EdgeMergeStrategy.maxMerge(this,weightProperty,default)
      case MergeStrategy.Min =>
        EdgeMergeStrategy.minMerge(this,weightProperty,default)
      case MergeStrategy.Product =>
        EdgeMergeStrategy.productMerge(this,weightProperty,default)
      case MergeStrategy.Average =>
        EdgeMergeStrategy.avgMerge(this,weightProperty,default)
      case MergeStrategy.Latest =>
        EdgeMergeStrategy.latestMerge(this,weightProperty,default)
      case MergeStrategy.Earliest =>
        EdgeMergeStrategy.earliestMerge(this,weightProperty, default)
    }
  }
  def totalWeight(f:Edge=>Float) : Float = {
    EdgeMergeStrategy.customMerge(this,f)
  }

  //send a message to the vertex on the other end of the edge
  def send(data: Any): Unit

}
