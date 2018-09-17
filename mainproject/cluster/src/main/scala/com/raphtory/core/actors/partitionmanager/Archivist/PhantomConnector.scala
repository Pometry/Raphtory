package com.raphtory.core.actors.partitionmanager.Archivist
import com.datastax.driver.core.SocketOptions
import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.database.{Database, DatabaseProvider}
import com.outworkers.phantom.dsl._
import com.raphtory.core.model.graphentities.Vertex

import scala.collection.mutable
import scala.collection.mutable.TreeMap
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

  def save(vertex:Vertex) = {
    val id = vertex.getId
    val oldestPoint = vertex.oldestPoint.get
    val history = createHistory(vertex.previousState)
    val query = s"INSERT INTO raphtory.vertexHistory (id, oldestpoint, history) VALUES (${id},${oldestPoint},$history);"
    println(query)
    session.execute(query)
  }

  private def createHistory(history: mutable.TreeMap[Long, Boolean]):String = {
    var s = "{"
    for((k,v) <- history){
      println(k)
      s = s+ s"$k : $v, "
    }
    s.dropRight(2) + "}"
  }

}



//CREATE TABLE raphtory.vertexhistory ( id bigint PRIMARY KEY, oldestPoint bigint, history map<bigint,boolean> );
//CREATE INDEX history_key ON raphtory.vertexhistory ( KEYS (history) );