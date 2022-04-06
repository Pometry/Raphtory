package com.raphtory.deployment

import java.net.InetAddress
import java.security.SecureRandom
import java.lang.{Byte => JByte}
import org.apache.commons.codec.binary.Hex
import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

// TODO PRINTS TO LOGS

object PythonGatewayServer {

  def writePortToFile(port: Int, gatewayServer: Py4JServer): Unit = {
    val connectionInfoPath = new File(sys.env("_RAPHTORY_DRIVER_CONN_INFO_PATH"))
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    dos.writeInt(port)

    val secretBytes = gatewayServer.getSecret.getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()
    if (!tmpPath.renameTo(connectionInfoPath)) {
      println(s"Unable to write connection information to $connectionInfoPath.")
      System.exit(1)
    }

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    println("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }

  def main(args: Array[String]): Unit = {
    val gatewayServer: Py4JServer = new Py4JServer()
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      println("failed to bind; exiting")
      System.exit(1)
    } else {
      println(s"Started PythonGatewayServer on port $boundPort")
    }

    writePortToFile(boundPort, gatewayServer)
  }
}

class Py4JServer() {

  def createSecret(): String = {
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](256 / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  private val secret: String = createSecret()
  private val localhost = InetAddress.getLoopbackAddress()
  private val server = new py4j.GatewayServer.GatewayServerBuilder()
    .authToken(secret)
    .javaPort(0)
    .javaAddress(localhost)
    .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
    .build()

  def getSecret(): String = secret

  def start(): Unit = server match {
    case gatewayServer: py4j.GatewayServer => gatewayServer.start()
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def getListeningPort: Int = server match {
    case gatewayServer: py4j.GatewayServer => gatewayServer.getListeningPort
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def shutdown(): Unit = server match {
    case gatewayServer: py4j.GatewayServer => gatewayServer.shutdown()
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }
}