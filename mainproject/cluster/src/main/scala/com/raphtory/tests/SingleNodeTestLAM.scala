package com.raphtory.tests

import akka.actor.{ActorSystem, Props}
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.clustersetup.singlenode.SingleNodeLAM
import com.raphtory.core.storage.controller.GraphRepoProxy
import com.twitter.util.Eval

object SingleNodeTestLAM extends  App{


  SingleNodeLAM("192.168.1.92:1600")

  def extra = {
    val eval = new Eval // Initializing The Eval without any target location

    val csvEval: Analyser = eval[Analyser]("import akka.actor.ActorContext\nimport com.raphtory.core.analysis.Analyser\nimport com.raphtory.core.storage.controller.GraphRepoProxy\n\nnew Analyser {\n  override implicit var context: ActorContext = _\n  override implicit var managerCount: Int = _\n\n  override def analyse()(implicit proxy: GraphRepoProxy.type, managerCount: Int): Any = \"hello\"\n\n  override def setup()(implicit proxy: GraphRepoProxy.type): Any = ???\n}")
    implicit val proxy: GraphRepoProxy.type = null
    implicit val managerCount: Int = 1
    import scala.io.Source

    val filename = "fileopen.scala"
    var code = ""
    for (line <- Source.fromFile("cluster/src/main/scala/"+this.getClass.getName.replaceAll("\\.","/").replaceAll("\\$",".scala")).getLines) {

      code+=s"$line\n"
    }
    println(code)
  }

}
