package com.raphtory.ethereumtest

sealed trait RaphtorySchema {
  def caseString(str: String): RaphtorySchema
}

case class EthereumTransaction(
    hash: String,
    nonce: Long,
    block_hash: String,
    block_number: Long,
    transaction_index: Long,
    from_address: String,
    to_address: String,
    value: Double,
    gas: Long,
    gas_price: Long,
    block_timestamp: Long,
    max_fee_per_gas: String,
    max_priority_fee_per_gas: String,
    transaction_type: Long
) extends RaphtorySchema {

  override def caseString(str: String): EthereumTransaction = {
    val fileLine = str.split(",")
    EthereumTransaction(
            fileLine(0),
            fileLine(1).toLong,
            fileLine(2),
            fileLine(3).toLong,
            fileLine(4).toLong,
            fileLine(5),
            fileLine(6),
            fileLine(7).toDouble,
            fileLine(8).toLong,
            fileLine(9).toLong,
            fileLine(11).toLong,
            fileLine(12),
            fileLine(13),
            fileLine(14).toLong
    )
  }
}

object EthereumTransaction {

  def apply(str: String): EthereumTransaction = {
    val fileLine = str.split(",")
    EthereumTransaction(
            fileLine(0),
            fileLine(1).toLong,
            fileLine(2),
            fileLine(3).toLong,
            fileLine(4).toLong,
            fileLine(5),
            fileLine(6),
            fileLine(7).toDouble,
            fileLine(8).toLong,
            fileLine(9).toLong,
            fileLine(11).toLong,
            fileLine(12),
            fileLine(13),
            fileLine(14).toLong
    )
  }
}
