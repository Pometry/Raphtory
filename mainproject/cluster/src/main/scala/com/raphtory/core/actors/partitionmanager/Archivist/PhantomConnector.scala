package com.raphtory.core.actors.partitionmanager.Archivist
import com.datastax.driver.core.SocketOptions
import com.outworkers.phantom.builder.query.RootSelectBlock
import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.database.{Database, DatabaseProvider}
import com.outworkers.phantom.dsl._
import com.raphtory.core.model.graphentities.Vertex
import com.raphtory.core.utils.{HistoryOrdering, Utils}

import scala.collection.mutable
import scala.collection.mutable.TreeMap
import scala.concurrent.Future

private object Connector {
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
  object vertexPropertyHistory extends VertexPropertyHistory with Connector
  object edgeHistory extends EdgeHistory with Connector


}
//bject RaphtoryDatabase extends Database(Connector.default)

object RaphtoryDB extends RaphtoryDatabase(Connector.default)

case class VertexPropertyPoint (id: Long, name:String, oldestPoint: Long, history:Map[Long, String])
case class VertexHistoryPoint (id: Long, oldestPoint: Long, history:Map[Long, Boolean])
case class EdgePropertyPoint (src: Int, dst:Int, name:String, oldestPoint: Long, history:Map[Long, String])
case class EdgeHistoryPoint (src: Int, dst:Int, oldestPoint: Long, history:Map[Long, Boolean])

abstract class VertexHistory extends Table[VertexHistory, VertexHistoryPoint] {
  object id extends LongColumn with PartitionKey
  object oldestPoint extends LongColumn
  object history extends MapColumn[Long,Boolean]

  def saveNew(id:Long,oldestPoint:Long,history:mutable.TreeMap[Long, Boolean]) = {
    session.execute(s"INSERT INTO raphtory.vertexHistory (id, oldestpoint, history) VALUES (${id},${oldestPoint},${createHistory(history)});")
  }

  def save(id:Long,history:mutable.TreeMap[Long,Boolean]) = {
    session.execute(s"UPDATE raphtory.vertexHistory SET history = history + ${createHistory(history)} WHERE id = $id;")
  }
  def createTable() = {
    try{session.execute("CREATE TABLE raphtory.vertexhistory ( id bigint PRIMARY KEY, oldestPoint bigint, history map<bigint,boolean> );")}
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("vertexHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.vertexhistory ;")
  }


  private def createHistory(history: mutable.TreeMap[Long, Boolean]):String = {
    var s = "{"
    for((k,v) <- history){
      s = s+ s"$k : $v, "
    }
    s.dropRight(2) + "}"
  }

  def allVertexHistory(id:Long) : Future[List[VertexHistoryPoint]] = {
    RaphtoryDB.vertexHistory.select.where(_.id eqs id).fetch()
  }
}


abstract class VertexPropertyHistory extends Table[VertexPropertyHistory, VertexPropertyPoint] {
  object id extends LongColumn with PartitionKey
  object name extends StringColumn with PartitionKey
  object oldestPoint extends LongColumn
  object history extends MapColumn[Long,String]

  def saveNew(id:Long,name:String,oldestPoint:Long,history:mutable.TreeMap[Long, String]) = {
    session.execute(s"INSERT INTO raphtory.VertexPropertyHistory (id, name, oldestpoint, history) VALUES (${id},'$name',${oldestPoint},${Utils.createPropHistory(history)});")
   //println(s"INSERT INTO raphtory.VertexPropertyHistory (id, name, oldestPoint, history) VALUES (${id},'$name',${oldestPoint},${createHistory(history)});")
  }

  def save(id:Long,name:String,history:mutable.TreeMap[Long,String]) = {
    session.execute(s"UPDATE raphtory.vertexPropertyHistory SET history = history + ${Utils.createPropHistory(history)} WHERE id = $id AND name = '$name';")
  }

  def createTable() = {
    try{session.execute(" CREATE TABLE raphtory.vertexpropertyhistory ( id bigint, name ascii, oldestPoint bigint, history map<bigint,ascii>, PRIMARY KEY (id,name) );")}
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("vertexPropertyHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.vertexpropertyhistory ;")
  }

  def allPropertyHistory(id:Long) : Future[List[VertexPropertyPoint]] = {
    RaphtoryDB.vertexPropertyHistory.select.where(_.id eqs id).fetch()
  }
}

abstract class EdgeHistory extends Table[EdgeHistory, EdgeHistoryPoint] {
  object src extends IntColumn with PartitionKey
  object dst extends IntColumn with PartitionKey
  object oldestPoint extends LongColumn
  object history extends MapColumn[Long,Boolean]

  def saveNew(src:Int,dst:Int,oldestPoint:Long,history:mutable.TreeMap[Long, Boolean]) = {
    session.execute(s"INSERT INTO raphtory.edgeHistory (src, dst, oldestpoint, history) VALUES (${src},$dst,${oldestPoint},${Utils.createHistory(history)});")
  }

  def save(id:Long,history:mutable.TreeMap[Long,Boolean]) = {
    session.execute(s"UPDATE raphtory.edgeHistory SET history = history + ${Utils.createHistory(history)} WHERE src = $src AND dst = $dst;")
  }
  def createTable() = {
    try{session.execute("CREATE TABLE raphtory.edgehistory ( src int, dst int, oldestPoint bigint, history map<bigint,boolean>, PRIMARY KEY (src,dst));")}
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("edgeHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.edgehistory ;")
  }


//  def allEdgeHistory(src:Int,dst:Int) : Future[List[EdgeHistoryPoint]] = {
//    RaphtoryDB.edgeHistory.select.where(_.src eqs src).where(_.dst eqs dst).fetch()
//  }
  def allOutgoingHistory(src:Int) : Future[List[EdgeHistoryPoint]] = {
    RaphtoryDB.edgeHistory.select.where(_.src eqs src).fetch()
  }
  def allIncomingHistory(dst:Int) : Future[List[EdgeHistoryPoint]] = {
    RaphtoryDB.edgeHistory.select.where(_.dst eqs dst).fetch()
  }
}


/*
 create keyspace raphtory with replication = {'class':'SimpleStrategy','replication_factor':1}; use raphtory;
 CREATE TABLE raphtory.vertexhistory ( id bigint PRIMARY KEY, oldestPoint bigint, history map<bigint,boolean> );
 CREATE INDEX history_key ON raphtory.vertexhistory ( KEYS (history) );

 CREATE TABLE raphtory.vertexpropertyhistory ( id bigint, name ascii, oldestPoint bigint, history map<bigint,ascii>, PRIMARY KEY (id,name) );


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
