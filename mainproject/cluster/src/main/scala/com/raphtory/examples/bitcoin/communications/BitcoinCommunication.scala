package com.raphtory.examples.bitcoin.communications

import com.raphtory.core.model.communication.SpoutGoing
import spray.json.JsValue

case class BitcoinTransaction(time:JsValue,blockID:JsValue,transaction:JsValue) extends SpoutGoing