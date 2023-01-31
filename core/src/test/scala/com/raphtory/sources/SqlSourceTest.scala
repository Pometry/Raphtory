package com.raphtory.sources

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.input.Graph
import com.raphtory.api.input.ImmutableString
import com.raphtory.api.input.MutableBoolean
import com.raphtory.api.input.MutableDouble
import com.raphtory.api.input.MutableFloat
import com.raphtory.api.input.MutableInteger
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.MutableString
import com.raphtory.api.input.Property
import com.raphtory.api.input.Type
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import munit.CatsEffectSuite

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant

class SqlSourceTest extends CatsEffectSuite {

  case class TestSqlConnection() extends SqlConnection {

    override def establish(): Connection = {
      Class.forName("org.h2.Driver")
      DriverManager.getConnection(s"jdbc:h2:mem:test")
    }
  }

  private val conn = TestSqlConnection()

  case class Row(
      index: Int,
      sourceId: Int,
      sourceName: String,
      targetId: Long,
      targetName: String,
      epoch: Long,
      time: Timestamp,
      boolean: Boolean,
      float: Float,
      double: Double,
      eType: String
  ) {

    override def toString: String =
      List(index, sourceId, sourceName, targetId, targetName, epoch, time, boolean, float, double, eType)
        .mkString("('", "', '", "')")
  }

  private def now(): Timestamp = Timestamp.from(Instant.now())

  private val rows = List(
          Row(0, 0, "Name0", 1, "Name1", 0, now(), true, 0.54f, 5.34564190789, "type0"),
          Row(1, 0, "Name0", 2, "Name2", 0, now(), true, 0.23f, 5.23564190789, "type0"),
          Row(2, 1, "Name1", 2, "Name2", 1, now(), false, 0.67f, 5.67564190789, "type1")
  )

  private val h2Database = ResourceSuiteLocalFixture(
          "H2 database connection",
          Resource.make(IO(DriverManager.getConnection("jdbc:h2:mem:test")))(conn => IO(conn.close())).map { conn =>
            val columns     = List(
                    "index INTEGER PRIMARY KEY",
                    "source_id INTEGER",
                    "source_name VARCHAR",
                    "target_id BIGINT",
                    "target_name VARCHAR",
                    "epoch BIGINT",
                    "time TIMESTAMP",
                    "boolean BIT",
                    "float REAL",
                    "double FLOAT",
                    "eType VARCHAR"
            )
            val createQuery =
              s"CREATE TABLE test (${columns.mkString(",")})"
            conn.createStatement().executeUpdate(createQuery)
            rows foreach { row =>
              conn.createStatement().executeUpdate(s"INSERT INTO test VALUES $row")
            }
          }
  )

  override def munitFixtures: Seq[Fixture[_]] = Seq(h2Database)

  test("SqlVertexSource ingest vertices from SQL table") {
    val source = SqlVertexSource(conn, s"select * from test", "source_id", "epoch")
    for {
      stream  <- source.makeStream[IO](0)
      updates <- stream.compile.toList.map(_.flatten.asInstanceOf[List[VertexAdd]])
      _       <- IO(assertEquals(updates.size, 3))
      _       <- IO(rows zip updates foreach {
                   case (row, update) =>
                     val expected = (row.sourceId, row.epoch)
                     val actual   = (update.srcId.toInt, update.updateTime)
                     assertEquals(actual, expected)
                 })
    } yield ()
  }

  test("SqlVertexSource ingest vertices with string ids, timestamp, properties, and type from SQL table") {
    val properties = List("target_id", "target_name", "boolean", "float", "double")
    val source     = SqlVertexSource(conn, s"select * from test", "source_name", "time", "etype", properties)
    for {
      stream  <- source.makeStream[IO](0)
      updates <- stream.compile.toList.map(_.flatten.asInstanceOf[List[VertexAdd]])
      _       <- IO(assertEquals(updates.size, 3))
      _       <- IO(rows zip updates foreach {
                   case (row, update) =>
                     val expectedProperties: Seq[Property] = Seq(
                             MutableLong("target_id", row.targetId),
                             MutableString("target_name", row.targetName),
                             MutableBoolean("boolean", row.boolean),
                             MutableFloat("float", row.float),
                             MutableDouble("double", row.double),
                             ImmutableString("name", row.sourceName)
                     )
                     val expected                          = (Graph.assignID(row.sourceName), row.time.getTime, Type(row.eType), expectedProperties)
                     val actualProperties                  = update.properties.properties
                     val actual                            = (update.srcId, update.updateTime, update.vType.get, actualProperties)
                     assertEquals(actual, expected)
                 })
    } yield ()
  }

  test("SqlEdgeSource ingest edges from SQL table") {
    val source = SqlEdgeSource(conn, s"select * from test", "source_id", "target_id", "epoch")
    for {
      stream  <- source.makeStream[IO](0)
      updates <- stream.compile.toList.map(_.flatten.asInstanceOf[List[EdgeAdd]])
      _       <- IO(assertEquals(updates.size, 3))
      _       <- IO(rows zip updates foreach {
                   case (row, update) =>
                     val expected = (row.sourceId, row.targetId, row.epoch)
                     val actual   = (update.srcId.toInt, update.dstId, update.updateTime)
                     assertEquals(actual, expected)
                 })
    } yield ()
  }

  test("SqlEdgeSource ingest edges with string ids, timestamp, properties, and type from SQL table") {
    val properties = List("target_id", "target_name", "boolean", "float", "double")
    val source     = SqlEdgeSource(conn, s"select * from test", "source_name", "target_name", "time", "etype", properties)
    for {
      stream     <- source.makeStream[IO](0)
      updates    <- stream.compile.toList.map(_.flatten)
      edgeUpdates = updates.collect {
                      case update: EdgeAdd => update
                    }
      _          <- IO(assertEquals(edgeUpdates.size, 3))
      _          <- IO(rows zip edgeUpdates foreach {
                      case (row, update) =>
                        val expectedProperties: Seq[Property] = Seq(
                                MutableLong("target_id", row.targetId),
                                MutableString("target_name", row.targetName),
                                MutableBoolean("boolean", row.boolean),
                                MutableFloat("float", row.float),
                                MutableDouble("double", row.double)
                        )
                        val expectedSrcId                     = Graph.assignID(row.sourceName)
                        val expectedDstId                     = Graph.assignID(row.targetName)
                        val expected                          = (expectedSrcId, expectedDstId, row.time.getTime, Type(row.eType), expectedProperties)
                        val actualProperties                  = update.properties.properties
                        val actual                            = (update.srcId, update.dstId, update.updateTime, update.eType.get, actualProperties)
                        assertEquals(actual, expected)
                    })
    } yield ()
  }
}
