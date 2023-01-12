package com.raphtory.sources

import java.sql.Connection
import java.sql.DriverManager

/** Base trait for database connections */
trait SqlConnection {
  def establish(): Connection
}

/** SqlConnection for Postgres databases
  *
  * @param database the name of the database
  * @param user the name of the user
  * @param password the password for the user
  * @param address the address of the host serving the database
  * @param port the port of the Postgres service (5432 by default)
  */
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

/** SqlConnection for Sqlite databases
  *
  * @param filepath the path of the file containing the database
  */
case class SqliteConnection(filepath: String) extends SqlConnection {

  override def establish(): Connection = {
    Class.forName("org.sqlite.JDBC")
    DriverManager.getConnection(s"jdbc:sqlite:$filepath")
  }
}
