package com.raphtory.core.build.client

import com.raphtory.core.components.management.ComponentFactory

class RaphtoryClient(leader:String,port:Int) {
  val system = ComponentFactory.initialiseActorSystem(List(leader),port)
}
