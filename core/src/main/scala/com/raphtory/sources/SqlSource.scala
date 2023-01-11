package com.raphtory.sources

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types
import scala.util.control.NonFatal

abstract class SqlSource(
    conn: SqlConnection,
    query: String
) extends Source {
  import SqlSource._

  override def makeStream[F[_]](implicit F: Async[F]): F[fs2.Stream[F, Seq[GraphAlteration.GraphUpdate]]] = {
    val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

    val resultSetResource = for {
      cn <- Resource.fromAutoCloseable(F.delay(conn.establish()))
      rs <- Resource.fromAutoCloseable(F.delay(cn.createStatement().executeQuery(buildSelectQuery(query))))
    } yield rs

    def getExtractor(rs: ResultSet) =
      for {
        columnTypes <- F.delay(getColumnTypes(rs.getMetaData))
        _           <- F.delay(logger.debug(s"Validating types present on the user SQL query"))
        _           <- F.delay(validateTypes(columnTypes))
        extractor   <- F.delay(buildExtractor(columnTypes))
      } yield extractor

    for {
      _              <- F.delay(logger.debug(s"Connecting to database with connection: '$conn'"))
      resultSetAlloc <- resultSetResource.allocated
      (rs, releaseRs) = resultSetAlloc
      extractor      <- getExtractor(rs).onError { case NonFatal(_) => releaseRs }
      _              <- F.delay(logger.debug(s"Defining stream of updates from SQL query"))
      stream         <- fs2.Stream
                          .iterate(0)(_ + 1)
                          .evalMap(index => F.delay(if (rs.next()) Some(extractor.apply(rs, index)) else None))
                          .collectWhile { case Some(updates) => updates }
                          .chunks
                          .map(_.toVector)
                          .onFinalize(releaseRs)
                          .pure[F]
    } yield stream
  }

  private def getColumnTypes(metadata: ResultSetMetaData): Map[String, Int] = {
    val indexes = 1 to metadata.getColumnCount
    indexes.map(index => (metadata.getColumnName(index).toUpperCase, metadata.getColumnType(index))).toMap
  }

  private def validateTypes(columnTypes: Map[String, Int]): Unit =
    expectedColumnTypes foreach {
      case (column, types) =>
        assert(checkType(columnTypes, column, types), s"Data type for column '$column' not supported")
    }

  private def buildSelectQuery(query: String): String =
    s"select ${expectedColumns.mkString(",")} from ($query) as query"

  protected def getPropertyBuilder(index: Int, col: String, columnTypes: Map[String, Int]): ResultSet => Property =
    if (checkType(columnTypes, col, boolTypes)) { (rs: ResultSet) => MutableBoolean(col, rs.getBoolean(index)) }
    else if (checkType(columnTypes, col, intTypes)) { (rs: ResultSet) => MutableInteger(col, rs.getInt(index)) }
    else if (checkType(columnTypes, col, longTypes)) { (rs: ResultSet) => MutableLong(col, rs.getLong(index)) }
    else if (checkType(columnTypes, col, floatTypes)) { (rs: ResultSet) => MutableFloat(col, rs.getFloat(index)) }
    else if (checkType(columnTypes, col, doubleTypes)) { (rs: ResultSet) => MutableDouble(col, rs.getDouble(index)) }
    else if (checkType(columnTypes, col, stringTypes)) { (rs: ResultSet) => MutableString(col, rs.getString(index)) }
    else throw new IllegalStateException(s"Unexpected type '${columnTypes(col)}' for column '$col'")

  protected def checkType(columnTypes: Map[String, Int], column: String, types: List[Int]): Boolean = {
    val upperCaseColumnTypes = columnTypes.map { case (column, cType) => (column.toUpperCase, cType) }
    types contains upperCaseColumnTypes(column.toUpperCase)
  }

  protected def expectedColumns: List[String]
  protected def expectedColumnTypes: Map[String, List[Int]]
  protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate
}

private[raphtory] object SqlSource {
  // Canonical types
  val boolTypes              = List(Types.BOOLEAN, Types.BIT)
  val intTypes: List[Int]    = List(Types.TINYINT, Types.SMALLINT, Types.INTEGER)
  val longTypes: List[Int]   = List(Types.BIGINT)
  val floatTypes: List[Int]  = List(Types.REAL)
  val doubleTypes: List[Int] = List(Types.FLOAT, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL)
  val stringTypes: List[Int] = List(Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR)
  val timeTypes: List[Int]   = List(Types.TIMESTAMP)

  // Semantic types
  val integerTypes: List[Int]  = intTypes ++ longTypes
  val idTypes: List[Int]       = integerTypes ++ stringTypes
  val epochTypes: List[Int]    = integerTypes ++ timeTypes
  val propertyTypes: List[Int] = boolTypes ++ integerTypes ++ floatTypes ++ doubleTypes ++ stringTypes
}
