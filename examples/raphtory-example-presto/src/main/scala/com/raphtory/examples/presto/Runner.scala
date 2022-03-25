package com.raphtory.examples.presto

import com.raphtory.deployment.RaphtoryGraph
import com.raphtory.examples.presto.spouts.EthereumPrestoSpout
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.schema.SchemaDefinition
import org.apache.pulsar.common.policies.data.RetentionPolicies

object Runner extends App {
  // Set unlimited retention to keep topic
  val retentionTime = -1
  val retentionSize = -1

  val admin         = PulsarAdmin.builder
    .serviceHttpUrl("http://localhost:8080")
    .tlsTrustCertsFilePath(null)
    .allowTlsInsecureConnection(false)
    .build
  val policies      = new RetentionPolicies(retentionTime, retentionSize)
  admin.namespaces.setRetention("public/default", policies)

  // Identify the schema
  val ethSchema: Schema[EthereumTransaction] = Schema.AVRO(
          SchemaDefinition
            .builder[EthereumTransaction]
            .withPojo(classOf[EthereumTransaction])
            .withAlwaysAllowNull(false)
            .build
  )
  // attach spout
  println("Creating spout...")
  val spout                                  = new EthereumPrestoSpout[EthereumTransaction](ethSchema, "ethereum_tx.csv")
  println("Running graph...")
  RaphtoryGraph[EthereumTransaction](spout)
}
