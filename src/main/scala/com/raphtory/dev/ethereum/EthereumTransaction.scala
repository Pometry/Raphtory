package com.raphtory.dev.ethereum

case class EthereumTransaction(
                                hash:               String,
                                nonce:              Long,
                                block_hash:         String,
                                block_number:       Long,
                                transaction_index:  Long,
                                from_address:       Option[String],
                                to_address:         Option[String],
                                value:              Double,
                                gas:                Long,
                                gas_price:          Long,
                                block_timestamp:    Long,
                                max_fee_per_gas:    Option[String],
                                max_priority_fee_per_gas: Option[String],
                                transaction_type:   Long
                              )

