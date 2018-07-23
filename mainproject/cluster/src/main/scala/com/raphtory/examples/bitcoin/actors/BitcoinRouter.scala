package com.raphtory.examples.bitcoin.actors

import com.raphtory.core.actors.router.RouterTrait

class BitcoinRouter(override val routerId:Int, override val initialManagerCount:Int) extends RouterTrait{

  override protected def otherMessages(rcvdMessage: Any): Unit = {
    parseTransaction(rcvdMessage)
  }

  def parseTransaction(value: Any): Unit = {

  }

}
