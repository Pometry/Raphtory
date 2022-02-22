package com.raphtory.core.components.spout.instance

import com.raphtory.core.components.spout.Spout

case class ResourceSpout(resource: String) extends Spout[String]
