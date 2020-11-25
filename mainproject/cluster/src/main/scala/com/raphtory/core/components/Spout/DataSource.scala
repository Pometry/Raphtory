package com.raphtory.core.components.Spout

import com.raphtory.core.model.communication.SpoutGoing



trait DataSource{
  private var dataComplete = false

  def setupDataSource():Unit

  def generateData():Option[SpoutGoing]

  def closeDataSource():Unit

  def dataSourceComplete():Unit = dataComplete=true
  def isComplete():Boolean = dataComplete
}
