package com.raphtory.deployment

import com.raphtory.config.ConfigHandler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import java.net.InetAddress
import java.security.SecureRandom
import java.lang.{Byte => JByte}
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

private[raphtory] class Py4JServer(entryPoint: Object) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private def writePortToFile(port: Int, conf: Config): Unit = {
    val filename = conf.getString("raphtory.python.gatewayFilePath")
    logger.info("Writing PythonGatewayServer details to file...")
    val connectionInfoPath = new File(filename)
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

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
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](256 / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  private val secret: String = createSecret()
  private val localhost = InetAddress.getLoopbackAddress()
  private val gatewayServer = new py4j.GatewayServer.GatewayServerBuilder(entryPoint)
    .authToken(secret)
    .javaPort(0)
    .javaAddress(localhost)
    .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
    .build()

  def getSecret(): String = secret

  def start(conf: Config): Unit = gatewayServer match {
    case gatewayServer: py4j.GatewayServer =>
      logger.info("Starting PythonGatewayServer...")
      gatewayServer.start()
      val boundPort: Int = gatewayServer.getListeningPort
      if (boundPort == -1) {
        logger.error("Failed to bind; Not running python gateway")
      } else {
        logger.info(s"Started PythonGatewayServer on port $boundPort")
        writePortToFile(boundPort, conf)
      }
    case other => logger.error(s"Start given unexpected Py4J gatewayServer ${other.getClass}")
  }

  def getListeningPort: Int = gatewayServer match {
    case gatewayServer: py4j.GatewayServer => gatewayServer.getListeningPort
    case other => logger.error(s"getListeningPort given unexpected Py4J gatewayServer ${other.getClass}"); -1
  }

  def shutdown(): Unit = gatewayServer match {
    case gatewayServer: py4j.GatewayServer => gatewayServer.shutdown()
    case other => logger.error(s"shutdown given unexpected Py4J gatewayServer ${other.getClass}")
  }
}

object Py4JServer {
  def apply(entryPoint: Object) =
    new Py4JServer(entryPoint)
}