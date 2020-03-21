package com.raphtory.examples.ldbc.routers

import com.raphtory.core.components.Router.RouterWorker

class LDBCRouter(routerId:Int, override val initialManagerCount:Int) extends RouterWorker{
  override protected def parseTuple(value: Any): Unit = {

  }
}
