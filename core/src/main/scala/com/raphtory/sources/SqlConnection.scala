package com.raphtory.sources

import java.sql.Connection
import java.sql.DriverManager

trait SqlConnection {
  def establish(): Connection
}

case class PostgresConnection(
    user: String,
    password: String,
    database: String,
    address: String = "localhost",
    port: Int = 5432
) extends SqlConnection {

  override def establish(): Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(s"jdbc:postgresql://$address:$port/$database", user, password)
  }
}

case class SqLiteConnection(filepath: String) extends SqlConnection {

  override def establish(): Connection = {
    Class.forName("SQLite.JDBCDriver")
    DriverManager.getConnection(s"jdbc:sqlite:$filepath")
  }
}
