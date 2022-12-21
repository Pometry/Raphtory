package com.raphtory.sources

import cats.effect.Async
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux

trait SqlConnection {
  def establish[F[_]: Async](): Aux[F, Unit]
}

case class PostgresConnection(
    user: String,
    password: String,
    database: String,
    address: String = "localhost",
    port: Int = 5432
) extends SqlConnection {

  override def establish[F[_]: Async](): Aux[F, Unit] =
    Transactor
      .fromDriverManager[F]("org.postgresql.Driver", s"jdbc:postgresql://$address:$port/$database", user, password)
}

case class SqLiteConnection(filepath: String) extends SqlConnection {

  override def establish[F[_]: Async](): Aux[F, Unit] =
    Transactor.fromDriverManager[F]("SQLite.JDBCDriver", s"jdbc:sqlite:$filepath")
}
