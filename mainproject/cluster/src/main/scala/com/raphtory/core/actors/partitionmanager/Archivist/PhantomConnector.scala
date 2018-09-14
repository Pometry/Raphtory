package com.raphtory.core.actors.partitionmanager.Archivist
import com.datastax.driver.core.SocketOptions


import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.database.{Database, DatabaseProvider}
import com.outworkers.phantom.dsl._

import scala.concurrent.Future

object Connector {
  val default: CassandraConnection = ContactPoint.local
    .withClusterBuilder(_.withSocketOptions(
      new SocketOptions()
        .setConnectTimeoutMillis(20000)
        .setReadTimeoutMillis(20000)
    )
    ).noHeartbeat().keySpace(
    KeySpace("raphtory").ifNotExists().`with`(
      replication eqs SimpleStrategy.replication_factor(1)
    )
  )
}

class RaphtoryDatabase(override val connector: CassandraConnection) extends Database[RaphtoryDatabase](connector) {

  object vertexHistory extends VertexHistory with Connector
}
//bject RaphtoryDatabase extends Database(Connector.default)

object RaphtoryDB extends RaphtoryDatabase(Connector.default)

case class VertexHistoryPoint (id: Long, time: Long, value: Boolean)

abstract class VertexHistory extends Table[VertexHistory, VertexHistoryPoint] {
  object id extends LongColumn with PartitionKey
  object time extends LongColumn with PartitionKey
  object value extends BooleanColumn

  def save(id:Long,time:Long,value:Boolean) = {
    insert
      .value(_.id, 3L)
      .value(_.time, 3L)
      .value(_.value, true)
  }

}
