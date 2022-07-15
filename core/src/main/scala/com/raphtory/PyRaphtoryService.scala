package com.raphtory

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.management.python.PythonGraphBuilder
import com.raphtory.spouts.FileSpout

import scala.io.Source
import scala.util.Using

object PyRaphtoryService extends RaphtoryService[String] {
  val ENV_SPOUT_FILE = "RAPHTORY_SPOUT_FILE"
  val ENV_GB_FILE    = "PYRAPHTORY_GB_FILE"
  val ENV_GB_CLASS   = "PYRAPHTORY_GB_CLASS"

  /** Defines type of Spout to be created for ingesting data
    */
  override def defineSpout(): Spout[String] = FileSpout(sys.env(ENV_SPOUT_FILE))

  /** Initialise `GraphBuilder` for building graphs */
  override def defineBuilder: GraphBuilder[String] = {
    val pyScriptFile = sys.env(ENV_GB_FILE)
    val pyScript     = Using(Source.fromFile(pyScriptFile))(src => src.mkString).get
    new PythonGraphBuilder[String](pyScript, sys.env(ENV_GB_CLASS))
  }
}
