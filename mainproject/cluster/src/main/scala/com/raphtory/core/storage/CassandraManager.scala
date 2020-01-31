package com.raphtory.core.storage

import java.net.InetAddress

import com.datastax.driver.core.SocketOptions
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.outworkers.phantom.connectors.CassandraConnection
import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl._
import com.raphtory.core.model.graphentities.{Edge, Vertex}
import com.raphtory.core.utils.Utils
import monix.execution.atomic.AtomicInt

import scala.collection.{mutable, parallel}
import scala.collection.parallel.mutable._
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Future
import scala.util.{Failure, Success}

class Connector {
  val default: CassandraConnection = ContactPoint.embedded
    .withClusterBuilder(_.withSocketOptions(
      new SocketOptions()
        .setConnectTimeoutMillis(20000)
        .setReadTimeoutMillis(20000)
    ).addContactPoint("raphtoryCassandra_cassandra-1").withPort(9042).addContactPoint("raphtoryCassandra_cassandra-2").withPort(9042)
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
  object edgePropertyHistory extends EdgePropertyHistory with Connector


}
//bject RaphtoryDatabase extends Database(Connector.default)

object RaphtoryDBWrite extends RaphtoryDatabase(new Connector().default){

  def clearDB() ={
    RaphtoryDBWrite.vertexHistory.clear()
    RaphtoryDBWrite.edgeHistory.clear()
    RaphtoryDBWrite.vertexPropertyHistory.clear()
    RaphtoryDBWrite.edgePropertyHistory.clear()
  }
  def createDB() = {
    RaphtoryDBWrite.vertexHistory.createKeySpace()
    RaphtoryDBWrite.vertexHistory.createTable()
    RaphtoryDBWrite.edgeHistory.createTable()
    RaphtoryDBWrite.vertexPropertyHistory.createTable()
    RaphtoryDBWrite.edgePropertyHistory.createTable()
  }


}

object RaphtoryDBRead extends RaphtoryDatabase(new Connector().default){

//  def retrieveVertex(id:Long,time:Long,vertexMap:ParTrieMap[Int, Vertex],completeCount:AtomicInt,retryQueue:parallel.mutable.ParHashSet[Long]):Unit={
//
//    val x = for {
//      vertexHistory <- RaphtoryDBRead.vertexHistory.allVertexHistory(id)
//      vertexPropertyHistory <- RaphtoryDBRead.vertexPropertyHistory.allPropertyHistory(id)
//      incomingedgehistory <- RaphtoryDBRead.edgeHistory.allIncomingHistory(id.toInt)
//      incomingedgePropertyHistory <- RaphtoryDBRead.edgePropertyHistory.allIncomingHistory(id.toInt)
//      outgoingedgehistory <- RaphtoryDBRead.edgeHistory.allOutgoingHistory(id.toInt)
//      outgoingedgePropertyHistory <- RaphtoryDBRead.edgePropertyHistory.allOutgoingHistory(id.toInt)
//    } yield {
//      val vertex = Vertex(vertexHistory.head,time)
//      if(!vertex.previousState.head._2) {
//        //println(s"${vertex.getId} apparently old at time $time $vertexHistory}")
//        throw EntityRemovedAtTimeException(vertex.getId)
//      }
//      for(property <-  vertexPropertyHistory){
//        vertex.addSavedProperty(property,time)
//      }
//      for(edgepoint <- incomingedgehistory){
//        try{vertex.addAssociatedEdge(Edge(edgepoint,time))}
//        catch {case e:EntityRemovedAtTimeException => } // edge was not alive at this point in time
//      }
//      for(edgepropertypoint <- incomingedgePropertyHistory){
//        vertex.incomingEdges.get(Utils.getEdgeIndex(edgepropertypoint.src,edgepropertypoint.dst)) match {
//          case Some(edge) =>edge.addSavedProperty(edgepropertypoint,time)
//          case None => //it was false at given point in time
//        }
//      }
//      for(edgepoint <- outgoingedgehistory){
//        try{vertex.addAssociatedEdge(Edge(edgepoint,time))}
//        catch {case e:EntityRemovedAtTimeException => } // edge was not alive at this point in time
//      }
//      for(edgepropertypoint <- outgoingedgePropertyHistory){
//        vertex.outgoingEdges.get(Utils.getEdgeIndex(edgepropertypoint.src,edgepropertypoint.dst)) match {
//          case Some(edge) =>edge.addSavedProperty(edgepropertypoint,time)
//          case None => //it was false at given point in time
//        }
//      }
//      vertex
//    }
//
//    x.onComplete(p => p match {
//      case Success(v)=> {
//        vertexMap.put(v.vertexId,v);
//        completeCount.increment()
//      }
//      case Failure(e) => {
//        e match {
//          case e:EntityRemovedAtTimeException => completeCount.increment()
//          case e:NoSuchElementException => completeCount.increment()
//          case e:NoHostAvailableException =>{retryQueue += id; completeCount.increment()}
//        }
//      } //do nothing
//    })
//
//  }


}




case class VertexPropertyPoint (id: Long, name:String, history:Map[Long, String])
case class VertexHistoryPoint (id: Long, history:Map[Long, Boolean])
case class EdgePropertyPoint (src: Long, dst:Long, name:String, history:Map[Long, String])
case class EdgeHistoryPoint (src: Long, dst:Long, history:Map[Long, Boolean])

abstract class VertexHistory extends Table[VertexHistory, VertexHistoryPoint] {
  object id extends LongColumn with PartitionKey
  object oldestPoint extends LongColumn
  object history extends MapColumn[Long,Boolean]

///  def saveNew(id:Long,oldestPoint:Long,history:mutable.TreeMap[Long, Boolean]) = {
 //   session.executeAsync(s"INSERT INTO raphtory.vertexHistory (id, oldestpoint, history) VALUES (${id},${oldestPoint},${Utils.createHistory(history)});")
 // }

  def save(id:Long,history:mutable.TreeMap[Long,Boolean]) = {
//    session.executeAsync(s"UPDATE raphtory.vertexHistory SET history = history + ${Utils.createHistory(history)} WHERE id = $id;")
    session.executeAsync(s"UPDATE raphtory.vertexHistory SET history = history + ${Utils.createHistory(history)} WHERE id = $id;")
  }
  def createKeySpace() = {
   try{session.execute("create keyspace raphtory with replication = {'class':'SimpleStrategy','replication_factor':1};")}
   catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("Keyspace already exists")}
  }

  def createTable() = {
    try{session.execute("CREATE TABLE raphtory.vertexhistory ( id bigint PRIMARY KEY, history map<bigint,boolean> );")}
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("vertexHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.vertexhistory ;")
  }

  def allVertexHistory(id:Long) : Future[List[VertexHistoryPoint]] = {
    RaphtoryDBWrite.vertexHistory.select.where(_.id eqs id).fetch()
  }
}


abstract class VertexPropertyHistory extends Table[VertexPropertyHistory, VertexPropertyPoint] {
  object id extends LongColumn with PartitionKey
  object name extends StringColumn with PartitionKey
  //object oldestPoint extends LongColumn
  object history extends MapColumn[Long,String]

 // def saveNew(id:Long,name:String,oldestPoint:Long,history:mutable.TreeMap[Long, String]) = {
 //   session.executeAsync(s"INSERT INTO raphtory.VertexPropertyHistory (id, name, oldestpoint, history) VALUES (${id},'$name',${oldestPoint},${Utils.createPropHistory(history)});")
   //println(s"INSERT INTO raphtory.VertexPropertyHistory (id, name, oldestPoint, history) VALUES (${id},'$name',${oldestPoint},${createHistory(history)});")
 // }

  def save(id:Long,name:String,history:mutable.TreeMap[Long,String]) = {
    session.executeAsync(s"UPDATE raphtory.vertexPropertyHistory SET history = history + ${Utils.createPropHistory(history)} WHERE id = $id AND name = '$name';")
  }

  def createTable() = {
    try{session.execute(" CREATE TABLE raphtory.vertexpropertyhistory ( id bigint, name ascii, history map<bigint,ascii>, PRIMARY KEY (id,name) );")}
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("vertexPropertyHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.vertexpropertyhistory ;")
  }

  def allPropertyHistory(id:Long) : Future[List[VertexPropertyPoint]] = {
    RaphtoryDBWrite.vertexPropertyHistory.select.where(_.id eqs id).fetch()
  }
}







abstract class EdgeHistory extends Table[EdgeHistory, EdgeHistoryPoint] {
  object src extends IntColumn with PartitionKey
  object dst extends IntColumn with PartitionKey
 // object oldestPoint extends LongColumn
 // object remote extends BooleanColumn
  object history extends MapColumn[Long,Boolean]

 // def saveNew(src:Int,dst:Int,oldestPoint:Long,remote:Boolean,history:mutable.TreeMap[Long, Boolean]) = {
 //   session.executeAsync(s"INSERT INTO raphtory.edgeHistory (src, dst, oldestpoint,remote, history) VALUES (${src},$dst,${oldestPoint},$remote,${Utils.createHistory(history)});")
 // }

  def save(src:Long,dst:Long,history:mutable.TreeMap[Long,Boolean]) = {
   // session.executeAsync(s"UPDATE raphtory.edgeHistory SET history = history + ${Utils.createHistory(history)} WHERE src = $src AND dst = $dst;")
    session.executeAsync(s"UPDATE raphtory.edgeHistory SET history = history + ${Utils.createHistory(history)} WHERE src = $src AND dst = $dst;")
  }
  def createTable() = {
    try{
      session.execute("CREATE TABLE raphtory.edgehistory ( src int, dst int, history map<bigint,boolean>, PRIMARY KEY (src,dst));")
      session.execute("create index on raphtory.edgehistory(dst);")
    }
    catch {case e:com.datastax.driver.core.exceptions.AlreadyExistsException => println("edgeHistory already exists")}
  }
  def clear() = {
    session.execute("truncate raphtory.edgehistory ;")
  }


//  def allEdgeHistory(src:Int,dst:Int) : Future[List[EdgeHistoryPoint]] = {
//    RaphtoryDB.edgeHistory.select.where(_.src eqs src).where(_.dst eqs dst).fetch()
//  }
  def allOutgoingHistory(src:Int) : Future[List[EdgeHistoryPoint]] = {
    RaphtoryDBWrite.edgeHistory.select.where(_.src eqs src).fetch()
  }
  def allIncomingHistory(dst:Int) : Future[List[EdgeHistoryPoint]] = {
    RaphtoryDBWrite.edgeHistory.select.where(_.dst eqs dst).fetch()
  }
}


abstract class EdgePropertyHistory extends Table[EdgePropertyHistory, EdgePropertyPoint] {

  object src extends IntColumn with PartitionKey

  object dst extends IntColumn with PartitionKey

  object name extends StringColumn with PartitionKey

//  object oldestPoint extends LongColumn

 // object remote extends BooleanColumn

  object history extends MapColumn[Long, String]

//  def saveNew(src: Int, dst: Int, name: String, oldestPoint: Long, remote: Boolean, history: mutable.TreeMap[Long, String]) = {
 //   session.executeAsync(s"INSERT INTO raphtory.EdgePropertyHistory (src, dst, name, oldestpoint,remote, history) VALUES (${src},$dst,'$name',${oldestPoint},$remote,${Utils.createPropHistory(history)});")
 // }

  def save(src: Long, dst: Long, name: String, history: mutable.TreeMap[Long, String]) = {
    session.executeAsync(s"UPDATE raphtory.EdgePropertyHistory SET history = history + ${Utils.createPropHistory(history)} WHERE src = $src AND dst = $dst AND name = '$name';")
    //session.executeAsync(s"UPDATE raphtory.EdgePropertyHistory SET history = history + ${Utils.createPropHistory(history)} WHERE src = $src AND dst = $dst AND name = '$name';")
  }

  def createTable() = {
    try {
      session.execute(" CREATE TABLE raphtory.EdgePropertyHistory ( src int, dst int, name ascii, history map<bigint,ascii>, PRIMARY KEY (src,dst,name) );")
      session.execute("create index on raphtory.EdgePropertyHistory(dst);")
    }
    catch {
      case e: com.datastax.driver.core.exceptions.AlreadyExistsException => println("EdgePropertyHistory already exists")
    }
  }

  def clear() = {
    session.execute("truncate raphtory.EdgePropertyHistory ;")
  }

  def allOutgoingHistory(src: Int): Future[List[EdgePropertyPoint]] = {
    RaphtoryDBWrite.edgePropertyHistory.select.where(_.src eqs src).fetch()
  }

  def allIncomingHistory(dst: Int): Future[List[EdgePropertyPoint]] = {
    RaphtoryDBWrite.edgePropertyHistory.select.where(_.dst eqs dst).fetch()
  }


}
