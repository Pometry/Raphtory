package com.raphtory.config

class SingleConnectorGateway(connector: Connector) extends Gateway {

  override protected def connectors: Map[(String, String), Connector] =
    Map(
            (defaultId, defaultId) -> connector
    )
}
