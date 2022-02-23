package com.raphtory.serialisers

import com.sksamuel.avro4s.AvroInputStream
import com.sksamuel.avro4s.AvroOutputStream
import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.Decoder
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.SchemaFor
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.schema.SchemaInfoProvider
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.pulsar.common.schema.SchemaType

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound

package object avro {

  class AvroPulsarSchema[T: Manifest: SchemaFor: Encoder: Decoder](
      // Note that this is a `var` because we need to implement `setSchemaInfoProvider`
      private var schemaInfoProvider: Option[SchemaInfoProvider] = None
  ) extends Schema[T] {

    private val generatedAvroSchema: org.apache.avro.Schema = AvroSchema[T]

    private def avroSchemaByVersion(schemaVersion: Option[Array[Byte]]): org.apache.avro.Schema = {
      val schemaFromVersion = for {
        provider        <- schemaInfoProvider
        version         <- schemaVersion
        // Pulsar's `SchemaInfoProvider`s use a local cache so calling `get` on the future should be ok.
        pulsarSchemaInfo = provider.getSchemaByVersion(version).get
        parser           = new org.apache.avro.Schema.Parser
      } yield parser.parse(pulsarSchemaInfo.getSchemaDefinition)
      schemaFromVersion.getOrElse(generatedAvroSchema)
    }

    override def supportSchemaVersioning: Boolean = true

    override def setSchemaInfoProvider(schemaInfoProvider: SchemaInfoProvider): Unit =
      this.schemaInfoProvider = Option(schemaInfoProvider)

    override def getSchemaInfo: SchemaInfo =
      SchemaInfoImpl
        .builder()
        .name(manifest[T].runtimeClass.getCanonicalName)
        .`type`(SchemaType.AVRO)
        .schema(generatedAvroSchema.toString.getBytes(StandardCharsets.UTF_8))
        .build()

    override def encode(t: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream
      val aos  = AvroOutputStream.binary[T].to(baos).build()
      try aos.write(t)
      finally aos.close()
      baos.toByteArray()
    }

    override def decode(bytes: Array[Byte], schemaVersionNullable: Array[Byte]): T = {
      val avroSchema = avroSchemaByVersion(Option(schemaVersionNullable))
      val ais        = AvroInputStream.binary[T].from(new ByteArrayInputStream(bytes)).build(avroSchema)
      try ais.iterator.next()
      finally ais.close()
    }

    override def clone(): Schema[T] = new AvroPulsarSchema(schemaInfoProvider)
  }

  @implicitNotFound("No Avro Schema for type ${T} found.")
  implicit def avroSchema[T: Manifest: SchemaFor: Encoder: Decoder]: Schema[T] =
    new AvroPulsarSchema[T]()
}
