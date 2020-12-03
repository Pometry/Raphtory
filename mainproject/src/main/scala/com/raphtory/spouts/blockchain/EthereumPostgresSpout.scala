package com.raphtory.spouts.blockchain

import cats.effect.{Blocker, IO}
import com.raphtory.core.actors.Spout.Spout
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor

import scala.collection.mutable

class EthereumPostgresSpout extends Spout[String]{
  var startBlock = System.getenv().getOrDefault("STARTING_BLOCK", "46147").trim.toInt //first block to have a transaction by default
  val batchSize  = System.getenv().getOrDefault("BLOCK_BATCH_SIZE", "100").trim.toInt //number of blocks to pull each query
  val maxblock   = System.getenv().getOrDefault("MAX_BLOCK", "8828337").trim.toInt    //Maximum block in database to stop querying once this is reached

  val dbURL      = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql:ether").trim //db connection string, default is for local with db called ether
  val dbUSER     = System.getenv().getOrDefault("DB_USER", "postgres").trim             //db user defaults to postgres
  val dbPASSWORD = System.getenv().getOrDefault("DB_PASSWORD", "").trim                 //default no password

  // querying done with doobie wrapper for JDBC (https://tpolecat.github.io/doobie/)
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)
  val dbconnector = Transactor.fromDriverManager[IO](
          "org.postgresql.Driver",
          dbURL,
          dbUSER,
          dbPASSWORD,
          Blocker.liftExecutionContext(ExecutionContexts.synchronous)
  )
  val queue = mutable.Queue[Option[String]]()

  override def generateData(): Option[String] = {
    if(queue isEmpty)
      pullBlocks()
    queue.dequeue()
  }

  protected def pullBlocks(): Unit = {
    sql"select from_address, to_address, value,block_timestamp from transactions where block_number >= $startBlock AND block_number < ${startBlock + batchSize} "
      .query[
              (String, String, String, String)
      ]                                      //get the to,from,value and time for transactions within the set block batch
      .to[List]                              // ConnectionIO[List[String]]
      .transact(dbconnector)                 // IO[List[String]]
      .unsafeRunSync                         // List[String]
      .foreach(x => queue+=(Some(x.toString()))) //send each transaction to the routers

    startBlock += batchSize//increment batch for the next query
    if (startBlock <= maxblock)
      dataSourceComplete()//if we have reached the max block we stop querying the database



  }

  override def setupDataSource(): Unit = {}
  override def closeDataSource(): Unit = {}
}
