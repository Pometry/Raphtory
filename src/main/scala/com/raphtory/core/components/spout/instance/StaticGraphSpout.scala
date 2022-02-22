package com.raphtory.core.components.spout.instance

import com.raphtory.core.components.spout.Spout

case class StaticGraphSpout(fileDataPath: String) extends Spout[String]
