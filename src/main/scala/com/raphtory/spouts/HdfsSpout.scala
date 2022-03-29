package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import com.typesafe.scalalogging.LazyLogging

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsSpout(headerUrlPort:String, route:String, dropHeader:Boolean=false) extends Spout[String] {

  val fs = FileSystem.get(new URI(headerUrlPort), new Configuration())
  private var status = fs.listStatus(new Path(route))
  private var hdfsManager = HDFSManager()

  override def setupDataSource(): Unit = {}

  override def closeDataSource(): Unit = {}

  override def generateData(): Option[String] = {
    val line = hdfsManager.nextLine()
    if (hdfsManager.finished(line)) {
      dataSourceComplete()
      None
    } else {
      Some(line)
    }
  }

  final case class HDFSManager private () extends LazyLogging {
    val stream = fs.open(status(0).getPath)
    status = status.slice(1,status.length)
    val bufferReader = new BufferedReader(new InputStreamReader(stream))

    def nextLine(): String = {
      bufferReader.readLine()
    }

    def finished(line: String): Boolean = {
      if (line == null) {
        if(status.length > 0) {
          hdfsManager = HDFSManager()
          logger.info("Still loading Spout...")
          false
        } else {
          logger.info("Spout loaded!")
          true
        }
      } else {
        false
      }
    }
  }
}