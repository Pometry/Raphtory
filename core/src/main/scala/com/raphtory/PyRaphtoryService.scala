package com.raphtory
import com.raphtory.api.input.{GraphBuilder, Spout}
import com.raphtory.internals.management.python.PythonGraphBuilder
import com.raphtory.spouts.FileSpout

object PyRaphtoryService extends RaphtoryService[String] {
  val ENV_SPOUT_FILE = "RAPHTORY_SPOUT_FILE"
  val ENV_GB_FILE = "PYRAPHTORY_GB_FILE"
  val ENV_GB_CLASS = "PYRAPHTORY_GB_CLASS"

  require(sys.env.contains(ENV_SPOUT_FILE), s"${ENV_SPOUT_FILE} variable needs to be set")
  require(sys.env.contains(ENV_GB_FILE), s"${ENV_GB_FILE} variable needs to be set to python file where graph builder class is declared")
  require(sys.env.contains(ENV_GB_CLASS), s"${ENV_GB_CLASS} variable needs to be set to python class")

  /** Defines type of Spout to be created for ingesting data
   */
  override def defineSpout(): Spout[String] = FileSpout(sys.env(ENV_SPOUT_FILE))



  /** Initialise `GraphBuilder` for building graphs */
  override def defineBuilder: GraphBuilder[String] = new PythonGraphBuilder[String](sys.env(ENV_GB_FILE), sys.env(ENV_GB_CLASS))
}
