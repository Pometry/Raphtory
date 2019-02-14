package com.raphtory.core.actors.partitionmanager.Writer.Helpers

import akka.actor.Actor
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage

class WritingSlave extends Actor {

  override def receive:Receive = {
    case VertexAdd(routerID,msgTime,srcId)                                => EntityStorage.vertexAdd(routerID,msgTime,srcId)
    case VertexRemoval(routerID,msgTime,srcId)                            => EntityStorage.vertexRemoval(routerID,msgTime,srcId)
    case VertexAddWithProperties(routerID,msgTime,srcId,properties)       => EntityStorage.vertexAdd(routerID,msgTime,srcId,properties)

    case EdgeAdd(routerID,msgTime,srcId,dstId)                            => EntityStorage.edgeAdd(routerID,msgTime,srcId,dstId)
    case RemoteEdgeAdd(routerID,msgTime,srcId,dstId,properties)           => EntityStorage.remoteEdgeAdd(routerID,msgTime,srcId,dstId,properties)
    case RemoteEdgeAddNew(routerID,msgTime,srcId,dstId,properties,deaths) => EntityStorage.remoteEdgeAddNew(routerID,msgTime,srcId,dstId,properties,deaths)
    case EdgeAddWithProperties(routerID,msgTime,srcId,dstId,properties)   => EntityStorage.edgeAdd(routerID,msgTime,srcId,dstId,properties)

    case EdgeRemoval(routerID,msgTime,srcId,dstId)                        => EntityStorage.edgeRemoval(routerID,msgTime,srcId,dstId)
    case RemoteEdgeRemoval(routerID,msgTime,srcId,dstId)                  => EntityStorage.remoteEdgeRemoval(routerID,msgTime,srcId,dstId)
    case RemoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths)        => EntityStorage.remoteEdgeRemovalNew(routerID,msgTime,srcId,dstId,deaths)

    case ReturnEdgeRemoval(routerID,msgTime,srcId,dstId)                  => EntityStorage.returnEdgeRemoval(routerID,msgTime,srcId,dstId)
    case RemoteReturnDeaths(msgTime,srcId,dstId,deaths)                   => EntityStorage.remoteReturnDeaths(msgTime,srcId,dstId,deaths)

  }

}
