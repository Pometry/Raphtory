package com.raphtory.core.analysis.API

import com.raphtory.core.model.graphentities.Edge

class EdgeVisitor(edge:Edge) {

  def getTimeAfter(time:Long) = edge.previousState.filter(k => k._1 >= time).minBy(x=>x._1)._1

}
