package com.raphtory.typedb

import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import com.vaticle.typedb.client.TypeDB
import com.vaticle.typedb.client.api.{TypeDBSession, TypeDBTransaction}
import com.vaticle.typeql.lang.TypeQL
import com.vaticle.typeql.lang.query.TypeQLInsert
import mjson.Json
import java.io.{FileInputStream, InputStreamReader}
import java.util

/**
 * Client to convert CSV files into TypeDB format
 */

abstract class Input(var path: String) {
  def getDataPath: String = path

  def template(data: Json): String
}

object CSVMigration {

  /**
   * 1. Creates a TypeDB instance
   * 2. Creates a session to the targeted database
   * 3. Initialises the list of inputs, each containing details required to parse the data
   * 4. Loads the csv data to TypeDB for each file
   * 5. Closes the session
   * 6. Closes the client
   *
   * @param inputs
   * @param databaseName
   * @param host (Default set to localhost:1729)
   */

  def connectAndMigrate(inputs: util.ArrayList[Input], databaseName: String, host: String = "localhost:1729"): Unit = {
    val client = TypeDB.coreClient(host)
    client.databases().create(databaseName)
    val session = client.session(databaseName, TypeDBSession.Type.DATA)

    inputs.forEach(input => loadDataIntoTypeDB(input, session))

    session.close()
    client.close()
  }

  /**
   *
   * This is your insert query into TypeDB. You can set the template you would like your data to be in.
   * More guidance on the TypeDB insert query schema is here: https://docs.vaticle.com/docs/query/insert-query
   */

  def initialiseInputs(path: String): util.ArrayList[Input] = {
    var inputs = new util.ArrayList[Input]()

    inputs.add(new Input(path){
      def template(company: Json) = s"insert ${company} isa company, has name ${company.at("name")};"
    })

    inputs.add(new Input(path) {
      def template(person: Json) = {
        s"insert $person isa person, has phone-number ${person.at("phone_number")};"
      }
    })

    inputs

  }


  /**
   * loads the csv data into our TypeDB database
   * 1. gets the data items as a list of json objects
   * 2. for each json object
   *  a. creates TypeDB transaction
   *  b. constructs the corresponding TypeQL insert query
   *  c. runs the query
   *  d. commits the transaction
   *  e. closes the transaction
   *
   * @param input
   * @param session
   */

  def loadDataIntoTypeDB(input: Input, session: TypeDBSession) = {

    val items = parseDataToJson(input)

    val transaction = session.transaction(TypeDBTransaction.Type.WRITE)

     items.forEach { item =>
       transaction.query.insert(TypeQL.parseQuery[TypeQLInsert](input.template(item)))
       transaction.commit()
     }

  }

  /**
   * 1. Reads csv file through a stream
   * 2. parses each row to a json object
   * 3. adds the json object to the list of items
   *
   * @param input
   */
  def parseDataToJson(input: Input): util.ArrayList[Json] = {
    val items = new util.ArrayList[Json]()

    val settings = new CsvParserSettings()
    settings.setLineSeparatorDetectionEnabled(true)
    val parser = new CsvParser(settings)
    parser.beginParsing(getReader(s"${input.path}.csv")) //1

    def hasNext = {
      parser.parseNext().nonEmpty
    }

    val columns = parser.parseNext()
    var row: Array[String] = Array[String]()
    while (hasNext) {
      val item = Json.`object`()
      var i = 0
      while(i < row.length) {
        i += 1
        item.set(columns(i), row(i))
      }
      items.add(item)
    }
    items
  }

  /**
   *
   * Method to stream in file
   *
   */
  def getReader(relativePath: String): InputStreamReader = {
    new InputStreamReader(new FileInputStream(relativePath))
  }

  /**
   * Main method to run the client.
   * @param databaseName
   * @param path
   */

  def main(args: Array[String]) = {
    val databaseName = ""
    val inputs = initialiseInputs("")
    connectAndMigrate(inputs, databaseName)
  }

}


