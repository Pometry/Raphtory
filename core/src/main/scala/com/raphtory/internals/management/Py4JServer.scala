package com.raphtory.internals.management

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import com.google.common.io.{ByteArrayDataOutput, ByteStreams}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Hex
import org.slf4j.LoggerFactory
import py4j.GatewayServer

import java.lang.{Byte => JByte}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.security.SecureRandom

/** Setups the Py4J Java server which allows Python to interact with Raphtory
 */
private[raphtory] class Py4JServer[F[_]](gatewayServer: GatewayServer)(implicit F: Sync[F]) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Similar to the PySpark implementation. This writes the port details of the
   * java gatway to a local file. When the python interface starts it reads this
   * local file to identity the port required to connect.
   */
  private def writePortToFile(port: Int, conf: Config): F[Unit] = {
    val filename = Paths.get(conf.getString("raphtory.python.gatewayFilePath"))

    // setup resources that need closing
    val channelR = for {
      tmpPath <- Py4JServer.createTmpFile(filename.getParent, filename)
      channel <-
        Resource.fromAutoCloseable(
          F.blocking(
            FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.READ)
          )
        )
    } yield channel

    // write content
    val writeContents = channelR.use { c =>
      F.blocking {
        logger.info("Writing PythonGatewayServer details to file...")
        val dos: ByteArrayDataOutput = ByteStreams.newDataOutput() //this doesn't need closing
        dos.writeInt(port)
        val secretBytes              = secret.getBytes(UTF_8)
        dos.writeInt(secretBytes.length)
        dos.write(secretBytes, 0, secretBytes.length)
        c.write(ByteBuffer.wrap(dos.toByteArray))
      }
    }

    writeContents
      .onError { t =>
        F.blocking(logger.error(s"Unable to write connection information to $filename.")) *> F
          .raiseError(t)
      }
      .flatMap { _ =>
        F.blocking(logger.info("Written PythonGatewayServer details to file."))
      }
  }

  private val secret = Py4JServer.secret

  /** This starts a java gateway server, and writes the port it binds to into a
   * local file.
   */
  def start(conf: Config): F[Unit] =
    F.blocking {
      logger.info("Starting PythonGatewayServer...")
      gatewayServer.start()
      gatewayServer.getListeningPort
    }.flatMap { boundPort =>
      F.blocking {
        if (boundPort == -1) {
          logger.error("Failed to bind; Not running python gateway")
          F.raiseError(new IllegalStateException("Unable to start Py4J Server"))
        }
        else {
          logger.info(s"Started PythonGatewayServer on port $boundPort")
          writePortToFile(boundPort, conf)
        }
      }
    }

  /** Getter to obtain the gateways listening port */
  def getListeningPort: F[Int] = F.blocking(gatewayServer.getListeningPort)

}

private[raphtory] object Py4JServer {

  def fromEntryPoint[F[_]: Sync](entryPoint: Object, config: Config): Resource[F, Py4JServer[F]] =
    for {
      py4jGateway <- makeGatewayServer(entryPoint)
      server      <-
        Resource.eval(Sync[F].delay(new Py4JServer[F](py4jGateway)).flatTap(_.start(config)))
    } yield server

  private def makeGatewayServer[F[_]](entryPoint: Object)(implicit F: Sync[F]) =
    Resource.make(F.blocking {
      new GatewayServer.GatewayServerBuilder()
        .entryPoint(entryPoint)
        .authToken(secret)
        .javaPort(0)
        .javaAddress(localhost)
        .callbackClient(GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
        .build()
    })(gateway => F.blocking(gateway.shutdown()))

  private def localhost = InetAddress.getLoopbackAddress

  private def secret: String = {
    val rnd         = new SecureRandom()
    val secretBytes = new Array[Byte](256 / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  def createTmpFile[F[_]](dir: Path, renameTo: Path)(implicit F: Sync[F]): Resource[F, Path] =
    Resource.make(
      F.blocking(
        Files
          .createTempFile(dir, "connection", ".info")
      )
    )(tmpFile => F.blocking(Files.move(tmpFile, renameTo)))
}
