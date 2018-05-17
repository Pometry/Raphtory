package com.raphtory.tests

import com.twitter.util.Eval
import java.io.File

import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.controller.GraphRepoProxy

object runtimeComplieTest extends App {

  val eval = new Eval // Initializing The Eval without any target location

  val csvEval: Analyser = eval[Analyser]("import akka.actor.ActorContext\nimport com.raphtory.core.analysis.Analyser\nimport com.raphtory.core.storage.controller.GraphRepoProxy\n\nnew Analyser {\n  override implicit var context: ActorContext = _\n  override implicit var managerCount: Int = _\n\n  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = \"hello\"\n\n  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ???\n}")
  implicit val proxy: GraphRepoProxy.type = null
  implicit val managerCount: Int = 1
  println(csvEval.analyse())

}
