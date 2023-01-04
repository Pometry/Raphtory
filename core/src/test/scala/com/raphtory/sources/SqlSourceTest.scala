package com.raphtory.sources

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
import munit.CatsEffectSuite

import java.sql.Connection
import java.sql.DriverManager

class SqlSourceTest extends CatsEffectSuite {

  case class Edge(source: Long, target: Long, time: Long)
  case class Vertex(id: Long, time: Long)

  case class TestSqlConnection() extends SqlConnection {

    override def establish(): Connection = {
      Class.forName("org.h2.Driver")
      DriverManager.getConnection(s"jdbc:h2:mem:test")
    }
  }

  private val conn = TestSqlConnection()

  private val h2Connection = ResourceSuiteLocalFixture(
          "H2 database connection",
          Resource.make(IO(DriverManager.getConnection("jdbc:h2:mem:test")))(conn => IO(conn.close()))
  )

  override def munitFixtures: Seq[Fixture[_]] = Seq(h2Connection)

  test("SqlEdgeSource ingest edges from SQL table") {
    val edges     = List(Edge(1, 2, 0), Edge(1, 3, 0), Edge(2, 3, 1))
    val edgeTable = "edges"
    val source    = SqlEdgeSource(conn, s"select * from $edgeTable", "source_id", "target_id", "time")

    def createTestTable(conn: Connection): Unit = {
      val createQuery                                             =
        s"CREATE TABLE $edgeTable (id SERIAL PRIMARY KEY, source_id INTEGER, target_id INTEGER, time INTEGER)"
      conn.createStatement().executeUpdate(createQuery)
      def updateQuery(sourceId: Long, targetId: Long, time: Long) =
        s"INSERT INTO $edgeTable (source_id, target_id, time) VALUES ($sourceId, $targetId, $time)"
      edges foreach { edge => conn.createStatement().executeUpdate(updateQuery(edge.source, edge.target, edge.time)) }
    }

    for {
      _           <- IO(createTestTable(h2Connection()))
      stream      <- source.makeStream[IO]
      updateLists <- stream.compile.toList
      updates      = updateLists.flatten
      sentEdges    = updates collect { case EdgeAdd(time, index, srcId, dstId, _, _) => Edge(srcId, dstId, time) }
      testPairs    = edges zip sentEdges
      _           <- IO(assertEquals(updates.size, 3))
      _           <- IO(testPairs foreach { case (expected, actual) => assertEquals(expected, actual) })
    } yield ()
  }

  test("SqlVertexSource ingest vertices from SQL table") {
    val vertices    = List(Vertex(1, 0), Vertex(2, 0), Vertex(5, 1))
    val vertexTable = "vertices"
    val source      = SqlVertexSource(conn, s"select * from $vertexTable", "id", "time")

    def createTestTable(conn: Connection): Unit = {
      val createQuery                       =
        s"CREATE TABLE $vertexTable (id INTEGER PRIMARY KEY, time INTEGER)"
      conn.createStatement().executeUpdate(createQuery)
      def updateQuery(id: Long, time: Long) =
        s"INSERT INTO $vertexTable (id, time) VALUES ($id, $time)"
      vertices foreach { vertex => conn.createStatement().executeUpdate(updateQuery(vertex.id, vertex.time)) }
    }

    for {
      _           <- IO(createTestTable(h2Connection()))
      stream      <- source.makeStream[IO]
      updateLists <- stream.compile.toList
      updates      = updateLists.flatten
      sentVertices = updates collect { case VertexAdd(time, index, id, properties, _) => Vertex(id, time) }
      testPairs    = vertices zip sentVertices
      _           <- IO(assertEquals(updates.size, 3))
      _           <- IO(testPairs foreach { case (expected, actual) => assertEquals(expected, actual) })
    } yield ()
  }
}
