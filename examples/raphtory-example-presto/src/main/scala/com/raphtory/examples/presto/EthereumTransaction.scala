package com.raphtory.examples.presto

case class EthereumTransaction(
                                hash:               String,
                                nonce:              Long,
                                block_hash:         String,
                                block_number:       Long,
                                transaction_index:  Long,
                                from_address:       String,
                                to_address:         String,
                                value:              Double,
                                gas:                Long,
                                gas_price:          Long,
                                block_timestamp:    Long,
                                max_fee_per_gas:    String,
                                max_priority_fee_per_gas: String,
                                transaction_type:   Long
                              )


