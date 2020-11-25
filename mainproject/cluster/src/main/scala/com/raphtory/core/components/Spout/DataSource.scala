package com.raphtory.core.components.Spout

import com.raphtory.core.model.communication.SpoutGoing

class NoDataAvailable extends Exception
class DataSourceComplete extends Exception

trait DataSource{
  def setupDataSource():Unit

  @throws(classOf[NoDataAvailable])
  @throws(classOf[DataSourceComplete])
  def generateData():SpoutGoing

  def closeDataSource():Unit
}
