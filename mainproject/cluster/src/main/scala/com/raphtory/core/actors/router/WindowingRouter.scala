package com.raphtory.core.actors.router

import scala.collection.concurrent.TrieMap

trait WindowingRouter extends  RouterTrait{
  val edgeWindow = TrieMap[Long,Long]()
  val vertexWindow = TrieMap[Int,Long]()


}
