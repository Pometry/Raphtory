package com.raphtory.examples.blockchain

import com.raphtory.core.analysis.API.WorkerID
import com.raphtory.core.model.communication.SpoutGoing
import spray.json.JsValue

import scala.collection.mutable.ArrayBuffer

case class BitcoinTransaction(time: JsValue, block: Int, blockID: JsValue, transaction: JsValue)  extends SpoutGoing
case class LitecoinTransaction(time: JsValue, block: Int, blockID: JsValue, transaction: JsValue) extends SpoutGoing

case class CoinsAquiredPayload(
    workerID: WorkerID,
    wallets: ArrayBuffer[(String, Double)],
    highestBlock: Int,
    blockhash: String
)
