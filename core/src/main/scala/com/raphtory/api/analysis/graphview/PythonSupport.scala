package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.table.Table

trait PythonSupport {

  type Graph

  def pythonStep(pickleStep: Array[Byte]): Graph

  def pythonIterate(pickleIterate: Array[Byte], iterations: Long, executeMessagedOnly: Boolean): Graph

  def pythonSelect(column: Array[Object]): Table

  def loadPythonScript(script:String) : Graph
}
