package com.raphtory.sources

import java.sql.Connection
import java.sql.DriverManager

trait SqlConnection {
  def establish(): Connection
}

case class PostgresConnection(
    database: String,
    user: String,
    password: String = "",
    address: String = "localhost",
    port: Int = 5432
) extends SqlConnection {

  override def establish(): Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(s"jdbc:postgresql://$address:$port/$database", user, password)
  }
}

case class SqliteConnection(filepath: String) extends SqlConnection {

  override def establish(): Connection = {
    Class.forName("org.sqlite.JDBC")
    DriverManager.getConnection(s"jdbc:sqlite:$filepath")
  }
}
