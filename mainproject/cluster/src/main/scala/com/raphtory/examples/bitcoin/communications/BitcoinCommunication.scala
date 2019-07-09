package com.raphtory.examples.bitcoin.communications

import com.raphtory.core.analysis.Worker
import com.raphtory.core.model.communication.SpoutGoing
import spray.json.JsValue

case class BitcoinTransaction(time:JsValue,block:Int,blockID:JsValue,transaction:JsValue) extends SpoutGoing

case class CoinsAquiredPayload(workerID:Worker, wallets: Vector[(String,Double)], highestBlock:Int, blockhash:String)