package com.raphtory.sources

import cats.effect.Async
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.typesafe.scalalogging.Logger
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux
import org.slf4j.LoggerFactory

import scala.util.Random

abstract class SqlSource(
    conn: SqlConnection,
    query: String,
    numColumns: Int
) extends Source {
  import SqlSource._

  override def makeStream[F[_]: Async]: F[fs2.Stream[F, Seq[GraphAlteration.GraphUpdate]]] = {
    val xa             = conn.establish[F]()
    val viewName       = s"raphtory_view_${Random.nextLong().abs}"
    val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

    for {
      _                <- Update(s"create view $viewName as ($query)").run(()).transact(xa)
      _                <- Async[F].delay(logger.debug(s"view $viewName for query '$query' created"))
      columns          <-
        HC.stream[Column](
                s"select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where UPPER(table_name) = '${viewName.toUpperCase}'",
                ().pure[PreparedStatementIO],
                512
        ).transact(xa)
          .compile
          .toList
      upperCaseColumns <-
        Async[F].delay(columns.map(col => Column(col.column_name.toUpperCase, col.data_type.toUpperCase)))
      _                <- Async[F].delay(validateColumns(upperCaseColumns, logger))
      tuples            = queryStringTuples(viewName, xa).evalTap(tuple => Async[F].delay(s"tuple: $tuple"))
      updates           = parseTuples(tuples, upperCaseColumns).evalTap(update => Async[F].delay(s"update: $update"))
    } yield updates.chunks.map(_.toVector)
  }

  private def queryStringTuples[F[_]: Async](viewName: String, xa: Aux[F, Unit]): fs2.Stream[F, List[String]] = {
    val select = buildSelectQuery(viewName)
    val query  = selectNColumns(numColumns)
    query.build(select).transact(xa).map(_.productIterator.toList.asInstanceOf[List[String]])
  }

  private def validateColumns(columns: List[Column], logger: Logger): Unit = {
    val columnNamesInTable = columns.map(_.column_name)
    val expectedColumns    = expectedColumnTypes.keys.toList
    logger.debug(s"Validating view with columns: $columnNamesInTable")
    expectedColumns foreach { column =>
      assert(columnNamesInTable contains column, s"Column '$column' not found in the result of the query")
    }
    columns filter (column => expectedColumns contains column.column_name) foreach { column =>
      assert(
              expectedColumnTypes(column.column_name) contains column.data_type,
              s"Data type for column ${column.column_name} not supported"
      )
    }
  }

  // TODO: abstract this more -> return just a list of columns to query
  protected def buildSelectQuery(viewName: String): String
  protected def expectedColumnTypes: Map[String, List[String]]

  protected def parseTuples[F[_]](
      tuples: fs2.Stream[F, List[String]],
      columns: List[Column]
  ): fs2.Stream[F, GraphUpdate]
}

private[raphtory] object SqlSource {
  case class Column(column_name: String, data_type: String)

  case class QueryBuilder[T: Read]() {
    def build(query: String): fs2.Stream[ConnectionIO, T] = HC.stream[T](query, ().pure[PreparedStatementIO], 512)
  }
  val integerTypes: List[String] = List("SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT")
  val stringTypes: List[String]             = List("CHARACTER", "VARCHAR", "CHARACTER VARYING")
  val idTypes: List[String]                 = integerTypes ++ stringTypes
  val epochTypes: List[String]              = integerTypes ++ List("TIMESTAMP")
  val allTypes: List[String]                = integerTypes ++ stringTypes ++ List("TIMESTAMP")
  private def sel[T: Read]: QueryBuilder[T] = QueryBuilder[T]

  def selectNColumns(n: Int) =
    n match {
      case 2  => sel[(String, String)]
      case 3  => sel[(String, String, String)]
      case 4  => sel[(String, String, String, String)]
      case 5  => sel[(String, String, String, String, String)]
      case 6  => sel[(String, String, String, String, String, String)]
      case 7  => sel[(String, String, String, String, String, String, String)]
      case 8  => sel[(String, String, String, String, String, String, String, String)]
      case 9  => sel[(String, String, String, String, String, String, String, String, String)]
      case 10 => sel[(String, String, String, String, String, String, String, String, String, String)]
      case 11 => sel[(String, String, String, String, String, String, String, String, String, String, String)]
      case 12 => sel[(String, String, String, String, String, String, String, String, String, String, String, String)]
      case 13 =>
        sel[(String, String, String, String, String, String, String, String, String, String, String, String, String)]
    }
}
