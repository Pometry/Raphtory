package com.raphtory.core.actors.partitionmanager.Archivist
import com.datastax.driver.core.SocketOptions
import com.outworkers.phantom.builder.query.RootSelectBlock
import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.database.{Database, DatabaseProvider}
import com.outworkers.phantom.dsl._
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.utils.HistoryOrdering

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

case class VertexHistoryPoint (id: Long, oldestPoint: Long, value: Boolean)

abstract class VertexHistory extends Table[VertexHistory, VertexHistoryPoint] {
  object id extends LongColumn with PartitionKey
  object time extends LongColumn
  object history extends MapColumn[Long,Boolean]

  def saveNew(id:Long,oldestPoint:Long,history:mutable.TreeMap[Long, Boolean]) = {
    session.execute(s"INSERT INTO raphtory.vertexHistory (id, oldestpoint, history) VALUES (${id},${oldestPoint},${createHistory(history)});")
  }

  def save(id:Long,history:mutable.TreeMap[Long,Boolean]) = {
    session.execute(s"UPDATE raphtory.vertexHistory SET history = history + ${createHistory(history)} WHERE id = $id;")
  }

  private def createHistory(history: mutable.TreeMap[Long, Boolean]):String = {
    var s = "{"
    for((k,v) <- history){
      println(k)
      s = s+ s"$k : $v, "
    }
    s.dropRight(2) + "}"
  }

  def allVertexHistory(id:Long) : Future[List[(Long, Map[Long, Boolean])]] = {
    RaphtoryDB.vertexHistory.select(_.id,_.history).where(_.id eqs id).fetch()
  }
}


/*
 create keyspace raphtory with replication = {'class':'SimpleStrategy','replication_factor':1}; use raphtory;
 CREATE TABLE raphtory.vertexhistory ( id bigint PRIMARY KEY, oldestPoint bigint, history map<bigint,boolean> );
 CREATE INDEX history_key ON raphtory.vertexhistory ( KEYS (history) );




 CREATE TABLE vertexHistory (
id bigint,
time bigint,
value boolean,
PRIMARY KEY (id, time)
)

INSERT INTO raphtory.vertexHistory (id, time, value)
  VALUES (1, 1,true)

INSERT INTO raphtory.vertexHistory (id, time, value)
  VALUES (1, 2,false)
* */
