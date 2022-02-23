package com.raphtory.core.components.spout.instance

import com.raphtory.core.components.spout.Spout
import org.apache.pulsar.client.api.Schema

class FileSpout[T](val source: String = "", val schema: Schema[T], val lineConverter: (String => T)) extends Spout[T]

object FileSpout {

  def apply[T](source: String, schema: Schema[T], lineConverter: (String => T)) =
    new FileSpout[T](source, schema, lineConverter)

  def apply(source: String = "") =
    new FileSpout[String](source, schema = Schema.STRING, lineConverter = s => s)
}
