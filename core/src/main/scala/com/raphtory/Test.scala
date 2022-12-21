package com.raphtory

import cats.effect.IO
import doobie._
import doobie.implicits._
import doobie.util.ExecutionContexts
import shapeless.record.Record
import cats.effect.unsafe.implicits.global
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.sinks.PrintSink
import com.raphtory.sources.PostgresConnection
import com.raphtory.sources.SqlEdgeSource

object Test extends RaphtoryApp.Local {

  override def run(args: Array[String], ctx: RaphtoryContext): Unit =
    ctx.runWithNewGraph() { graph =>
      val conn = PostgresConnection("postgres", "password", "test2")
      graph.load(SqlEdgeSource(conn, "select * from graph", "source_id", "target_id", "time"))
      graph.execute(ConnectedComponents).get().foreach { table =>
        table.rows.toList.foreach(println(_))
      }
    }

//  def insert2_H2(name: String, age: Option[Short]): ConnectionIO[Person] =
//    for {
//      id <- sql"insert into person (name, age) values ($name, $age)".update
//              .withUniqueGeneratedKeys[Int]("id")
//      p  <- sql"select id, name, age from person where id = $id"
//              .query[()]
//              .unique
//    } yield p

}
