package com.raphtory.typedb

import com.raphtory.Raphtory
import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance
import com.typesafe.config.Config
import com.vaticle.typedb.client.TypeDB
import com.vaticle.typedb.client.api.answer.ConceptMap
import com.vaticle.typedb.client.api.TypeDBSession
import com.vaticle.typedb.client.api.TypeDBTransaction
import com.vaticle.typeql.lang.TypeQL

import java.util.stream

/** A [[com.raphtory.api.input.Spout Spout]] that reads files from disk.
  *
  * This Spout connects to a TypeDB database and streams out data from the database,
  * inserting it into Raphtory for analysis. You will need to write a TypeDB match
  * to how your schema looks like in your database.
  *
  * @param database The database in TypeDB that you would like to connect to.
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.FileSink
  * import com.raphtory.spouts.TypeDBSpout
  *
  * val webSpout = new TypeDBSpout("database")
  * val graph = Raphtory.load(TypeDBSpout, YourGraphBuilder())
  * val sink = FileSink("/tmp/raphtoryTest")
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[com.raphtory.api.input.Spout Spout]]
  *      [[com.raphtory.Raphtory Raphtory]]
  */
case class TypeDBSpout(database: String) extends Spout[String] {
  override def buildSpout(): SpoutInstance[String] = new TypeDBSpoutInstance(database)
}

class TypeDBSpoutInstance(database: String) extends SpoutInstance[String] {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()
  // 1. Connect to TypeDB server using client
  private val client         = TypeDB.coreClient("localhost:1729")
  // 2. Connect to particular database via a session to carry out transactions
  private val session        = client.session(database, TypeDBSession.Type.DATA)

  private var lines: stream.Stream[ConceptMap] = _
  private var item: String                     = _

  try {
    // 3. Make a transaction which performs a query or Concept API call on database
    val readTransaction = session.transaction(TypeDBTransaction.Type.READ)

    // 4. Get query by matching the schema to your use case

    //a. Matching instances of an entity type, you can limit the number of answers you receive.
    val schema = TypeQL.`match`(TypeQL.`var`("p").isa("person")).get("p").limit(10)

    lines = readTransaction.query().`match`(schema)
  }
  catch {
    case e: Exception =>
      logger.error(s"NullPointerException: Check TypeDB source")
      throw e
  }

  override def spoutReschedules(): Boolean = true

  override def hasNext: Boolean = lines.findAny().isPresent

  override def next(): String =
    try {
      lines.forEach(line =>
        // Get item as Thing, Attribute or Entity Type
        //Attribute: info that determines property of element e.g. name, language, age
        // getIID retrieves the unique id of the type
        item = line.get("name").asThing().getIID
      )
      item
    }
    catch {
      case e: Exception =>
        logger.error(s"Failed to get value")
        throw e
    }

  override def close() =
    client.close()
}
