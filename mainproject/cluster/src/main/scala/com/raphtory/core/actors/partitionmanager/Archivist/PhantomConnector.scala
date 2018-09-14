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

  def save() = {
    insert
      .value(_.id, 3L)
      .value(_.time, 3L)
      .value(_.value, true).future()
  }

}
class CassandraDatabase(override val connector: CassandraConnection) extends Database[CassandraDatabase](connector) {
  object beers extends Beers with Connector
}
object CassandraDatabase extends CassandraDatabase(ContactPoint.local.keySpace(KeySpace("outworkers")
  .ifNotExists().`with`(replication eqs SimpleStrategy.replication_factor(1))))

case class Beer(company:String,name:String,style:String)

abstract class Beers extends Table[Beers, Beer] {
  object company extends StringColumn with PartitionKey
  object name extends StringColumn with PartitionKey
  object style extends StringColumn

  def save(beer: Beer): Future[ResultSet] = {
    insert
      .value(_.company, beer.company)
      .value(_.name, beer.name)
      .value(_.style, beer.style)
      .future()
  }

  def all(): Future[Seq[Beer]] = {
    select.all.fetch()
  }
}