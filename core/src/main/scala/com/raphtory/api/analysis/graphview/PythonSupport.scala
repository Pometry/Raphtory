package com.raphtory.api.analysis.graphview

import com.raphtory.api.analysis.table.Table

trait PythonSupport {

  type Graph

  def pythonStep(pickleStep: Array[Byte]): Graph

  def pythonIterate(pickleIterate: Array[Byte], iterations: Long, executeMessagedOnly: Boolean): Graph

  /**
    * pemja and py4j provide different objects here
    * one sends an [[java.util.ArrayList]] the other plain [[Array]]
    *
    * @param columns
    * @return
    */
  def pythonSelect(column: Object): Table

  def loadPythonScript(script: String): Graph

  def pythonSetGlobalState(pickleState: Array[Byte]): Graph
}
