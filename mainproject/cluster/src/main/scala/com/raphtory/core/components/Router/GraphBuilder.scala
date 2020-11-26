package com.raphtory.core.components.Router

import com.raphtory.core.model.communication.GraphUpdate

import scala.collection.mutable
import scala.collection.parallel.mutable.ParHashSet

abstract class GraphBuilder[T] {

  var updates:mutable.HashSet[GraphUpdate] = mutable.HashSet[GraphUpdate]()

//  protected def parseTuple(tuple: T):


}
