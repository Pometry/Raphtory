package com.raphtory.examples.gabMining.analysis
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy, VertexVisitor}
import akka.actor.ActorContext

class GabMiningVandE extends Analyser {
  override def analyse(): Any = {
    println("*********INSIDE ANALYSER: ")
   //println( proxy.getVerticesSet())
    proxy.getTotalEdgesSet().size
//    proxy.getTotalVerticesSet().foreach(v => {
//
//      val vertex = proxy.getVertex(v)
//
//      println("**********Total edges :" + v)
//
//    })
  }
  override def setup(): Any = {

  }
}
