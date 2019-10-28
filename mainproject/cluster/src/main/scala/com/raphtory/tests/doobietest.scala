package com.raphtory.tests
import cats.effect.{Blocker, IO}
import doobie.util.transactor.Transactor
import doobie.implicits._
import doobie.util.ExecutionContexts


object doobietest extends App {


  // We need a ContextShift[IO] before we can construct a Transactor[IO]. The passed ExecutionContext
  // is where nonblocking operations will be executed. For testing here we're using a synchronous EC.
  implicit val cs = IO.contextShift(ExecutionContexts.synchronous)
  // A transactor that gets connections from java.sql.DriverManager and executes blocking operations
  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",     // driver classname
    "jdbc:postgresql:ether",     // connect URL (driver-specific)
    "postgres",                  // user
    "",                          // password
    Blocker.liftExecutionContext(ExecutionContexts.synchronous)
  )
  val program2 = sql"select from_address, to_address, value,block_timestamp from transactions where block_number >= 46147 AND block_number < 46247 ".query[(String,String,String,String)]
    .to[List]         // ConnectionIO[List[String]]
    .transact(xa)     // IO[List[String]]
    .unsafeRunSync    // List[String]
    println(program2)
}
