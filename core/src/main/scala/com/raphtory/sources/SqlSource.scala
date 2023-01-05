package com.raphtory.sources

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.input.MutableInteger
import com.raphtory.api.input.MutableLong
import com.raphtory.api.input.MutableString
import com.raphtory.api.input.Property
import com.raphtory.api.input.Source
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.sources.SqlSource.isInt
import com.raphtory.sources.SqlSource.isLong
import com.raphtory.sources.SqlSource.isString
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.Types

abstract class SqlSource(
    conn: SqlConnection,
    query: String
) extends Source {

  override def makeStream[F[_]](implicit F: Async[F]): F[fs2.Stream[F, Seq[GraphAlteration.GraphUpdate]]] = {
    val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

    val resultSet = for {
      cn <- Resource.fromAutoCloseable(F.delay(conn.establish()))
      rs <- Resource.fromAutoCloseable(F.delay(cn.createStatement().executeQuery(buildSelectQuery(query))))
    } yield rs

    for {
      resultSetAlloc <- resultSet.allocated
      (rs, releaseRs) = resultSetAlloc
      columnTypes     = getColumnTypes(rs.getMetaData)
      _              <- F.delay(validateTypes(columnTypes))
      extractor       = buildExtractor(columnTypes)
      stream         <- fs2.Stream
                          .iterate(0)(_ + 1)
                          .evalMap(index => F.delay(if (rs.next()) Some(extractor.apply(rs, index)) else None))
                          .collectWhile { case Some(updates) => updates }
                          .chunks
                          .map(_.toVector)
                          .onFinalize(releaseRs)
                          .pure[F]
      // TODO we need to release rs if something goes wrong before defining the stream!!!!!
    } yield stream
  }

  private def getColumnTypes(metadata: ResultSetMetaData): Map[String, Int] = {
    val indexes = 1 to metadata.getColumnCount
    indexes.map(index => (metadata.getColumnName(index).toUpperCase, metadata.getColumnType(index))).toMap
  }

  private def validateTypes(columnTypes: Map[String, Int]): Unit =
    expectedColumnTypes foreach {
      case (column, types) =>
        assert(types contains columnTypes(column), s"Data type for column '$column' not supported")
    }

  private def buildSelectQuery(query: String): String =
    s"select ${expectedColumns.mkString(",")} from ($query)"

  protected def getPropertyBuilder(index: Int, column: String, columnTypes: Map[String, Int]): ResultSet => Property =
    if (isInt(columnTypes(column))) { (rs: ResultSet) => MutableInteger(column, rs.getInt(index)) }
    else if (isLong(columnTypes(column))) { (rs: ResultSet) => MutableLong(column, rs.getLong(index)) }
    else if (isString(columnTypes(column))) { (rs: ResultSet) => MutableString(column, rs.getString(index)) }
    else throw new IllegalStateException(s"Unexpected type '${columnTypes(column)}' for column '$column'")

  protected def expectedColumns: List[String]
  protected def expectedColumnTypes: Map[String, List[Int]]
  protected def buildExtractor(columnTypes: Map[String, Int]): (ResultSet, Long) => GraphUpdate
}

private[raphtory] object SqlSource {
  val intTypes: List[Int]      = List(Types.TINYINT, Types.SMALLINT, Types.INTEGER)
  val longTypes: List[Int]     = List(Types.BIGINT)
  val integerTypes: List[Int]  = intTypes ++ longTypes
  val stringTypes: List[Int]   = List(Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR)
  val timeTypes: List[Int]     = List(Types.TIMESTAMP)
  val idTypes: List[Int]       = integerTypes ++ stringTypes
  val epochTypes: List[Int]    = integerTypes ++ timeTypes
  val propertyTypes: List[Int] = integerTypes ++ stringTypes

  def isInt(tp: Int): Boolean    = intTypes contains tp
  def isLong(tp: Int): Boolean   = longTypes contains tp
  def isString(tp: Int): Boolean = stringTypes contains tp
}
