package com.raphtory.internal.management

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import py4j.GatewayServer

import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream
import java.lang.{Byte => JByte}
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.SecureRandom

/** Setups the Py4J Java server which allows Python to interact with Raphtory
  */
class Py4JServer(entryPoint: Object) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Similar to the PySpark implementation. This writes the port details of the
    * java gatway to a local file. When the python interface starts it reads this
    * local file to identity the port required to connect.
    */
  private def writePortToFile(port: Int, conf: Config): Unit = {
    val filename           = conf.getString("raphtory.python.gatewayFilePath")
    logger.info("Writing PythonGatewayServer details to file...")
    val connectionInfoPath = new File(filename)
    val tmpPath            = Files
      .createTempFile(connectionInfoPath.getParentFile().toPath(), "connection", ".info")
      .toFile()

    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    dos.writeInt(port)

    val secretBytes = getSecret().getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()
    if (!tmpPath.renameTo(connectionInfoPath)) {
      logger.error(s"Unable to write connection information to $connectionInfoPath.")
      return
    }
    logger.info("Written PythonGatewayServer details to file.")
  }

  private def createSecret(): String = {
    val rnd         = new SecureRandom()
    val secretBytes = new Array[Byte](256 / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  private val secret: String = createSecret()
  private val localhost      = InetAddress.getLoopbackAddress()

  private lazy val gatewayServer: GatewayServer = new GatewayServer.GatewayServerBuilder()
    .entryPoint(entryPoint)
    .authToken(secret)
    .javaPort(0)
    .javaAddress(localhost)
    .callbackClient(GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
    .build()
  private var startedServer                     = false

  /** Getter for the secret
    * @return Secret
    */
  def getSecret(): String = secret

  /** This starts a java gateway server, and writes the port it binds to into a
    * local file.
    */
  def start(conf: Config): Unit =
    gatewayServer match {
      case gatewayServer: GatewayServer =>
        if (!startedServer) {
          logger.info("Starting PythonGatewayServer...")
          gatewayServer.start()
          val boundPort: Int = gatewayServer.getListeningPort
          if (boundPort == -1)
            logger.error("Failed to bind; Not running python gateway")
          else {
            logger.info(s"Started PythonGatewayServer on port $boundPort")
            writePortToFile(boundPort, conf)
            startedServer = true
          }
        }
      case other                        => logger.error(s"Start given unexpected Py4J gatewayServer ${other.getClass}")
    }

  /** Getter to obtain the gateways listening port */
  def getListeningPort: Int =
    gatewayServer match {
      case gatewayServer: GatewayServer => gatewayServer.getListeningPort
      case other                        =>
        logger.error(s"getListeningPort given unexpected Py4J gatewayServer ${other.getClass}"); -1
    }

  /** Safely shutsdown the gateway server */
  def shutdown(): Unit =
    gatewayServer match {
      case gatewayServer: GatewayServer => gatewayServer.shutdown(); startedServer = false
      case other                        => logger.error(s"shutdown given unexpected Py4J gatewayServer ${other.getClass}")
    }
}

object Py4JServer {

  def apply(entryPoint: Object) =
    new Py4JServer(entryPoint: Object)
}
