package com.raphtory.test.allcommands

import com.raphtory.core.actors.Spout.Spout

import scala.util.Random

class AllCommandsSpout extends Spout[String] {

  var totalCount = 100
  var freq = 1000
  var pool = System.getenv().getOrDefault("ENTITY_POOL", "10000").toInt
  var msgID = 0
  val rnd = new scala.util.Random(123)
  override def setupDataSource(): Unit = {}

  override def generateData(): Option[String] ={
    if(msgID<=300000)
      Some(distribution())
    else {
      dataSourceComplete()
      None
    }
  }


  override def closeDataSource(): Unit = {}


  def distribution(): String = {
    val random = rnd.nextFloat()
    if (random <= 0.3) genVertexAdd()
    else genEdgeAdd()
//    else if (random <= 0.7) genEdgeAdd()
//    else genEdgeRemoval()
//    else if (random <= 0.98) genEdgeRemoval()
//    else genVertexRemoval()
  }

  def genVertexAdd(): String =
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID()}, ${genProperties(2)}}}"""

  def genVertexAdd(src: Int): String = //overloaded method if you want to specify src ID
    s""" {"VertexAdd":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2)}}}"""

  def genVertexUpdateProperties(): String =
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(1)}}}"""

  def genVertexUpdateProperties(src: Int): String = //overloaded to mass src
    s""" {"VertexUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genProperties(2)}}}"""

  def genVertexRemoval(): String =
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID()}}}"""

  def genVertexRemoval(src: Int): String =
    s""" {"VertexRemoval":{${getMessageID()}, ${genSrcID(src)}}}"""

  def genEdgeAdd(): String =
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID()}, ${genDstID()}, ${genProperties(2)}}}"""

  def genEdgeAdd(src: Int, dst: Int): String =
    s""" {"EdgeAdd":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""

  def genEdgeUpdateProperties(): String =
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID()}, ${genDstID()}, ${genProperties(2)}}}"""

  def genEdgeUpdateProperties(src: Int, dst: Int): String =
    s""" {"EdgeUpdateProperties":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}, ${genProperties(2)}}}"""

  def genEdgeRemoval(): String =
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID()}, ${genDstID()}}}"""

  def genEdgeRemoval(src: Int, dst: Int): String =
    s""" {"EdgeRemoval":{${getMessageID()}, ${genSrcID(src)}, ${genDstID(dst)}}}"""

  def genSetSrcID(): String = s""" "srcID":9 """

  def genSetDstID(): String = s""" "dstID":10 """

  def genSrcID(): String = s""" "srcID":${rnd.nextInt(pool)} """

  def genDstID(): String = s""" "dstID":${rnd.nextInt(pool)} """

  def genSrcID(src: Int): String = s""" "srcID":$src """

  def genDstID(dst: Int): String = s""" "dstID":$dst """

  def getMessageID(): String = {
    msgID += 1
    s""" "messageID":$msgID """
  }

  def genProperties(numOfProps: Int): String = {
    var properties = "\"properties\":{"
    for (i <- 1 to numOfProps) {
      val propnum = i
      if (i < numOfProps) properties = properties + s""" "property$propnum":"${"test"}", """
      else properties = properties + s""" "property$propnum":"${"test"}" }"""
    }
    properties
  }

}
