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
  def pythonSelect(column: Iterable[String]): Table

  def pythonSelectState(column: Object): Table

  def pythonStepState(pyObj: Array[Byte]): Graph

  def loadPythonScript(script: String): Graph

  def pythonSetGlobalState(pyObj: Array[Byte]): Graph

  def pythonGlobalSelect(pyObj: Array[Byte]): Table
}
