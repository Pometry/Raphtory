package com.raphtory.spouts

import com.raphtory.api.input.Spout
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.{HttpUriRequest, RequestBuilder}
import org.apache.http.impl.client.HttpClients
import java.io.{BufferedReader, InputStream, InputStreamReader}

class WebSocketSpout(url: String, auth: Option[String], contentType: Option[String]) extends Spout[String] {

  val client = HttpClients.custom().build()
  var request: HttpUriRequest = _
  if (auth.nonEmpty && contentType.nonEmpty) {
    request = RequestBuilder.get().setUri(url)
      .setHeader(HttpHeaders.AUTHORIZATION, auth.get)
      .addHeader(HttpHeaders.CONTENT_TYPE, contentType.get)
      .build()
  } else {
    request = RequestBuilder.get().setUri(url).build()
  }

  var input: String = _
  val execute = client.execute(request)
  val inStream: InputStream = execute.getEntity.getContent
  val bufferedReader = new BufferedReader(new InputStreamReader(execute.getEntity.getContent))
  val stringBuilder = new StringBuilder()

  override def spoutReschedules(): Boolean = true

  override def hasNext: Boolean = input.nonEmpty

  override def next(): String = {
    try {
      input = bufferedReader.readLine()
      stringBuilder.append(input)
      input
    } catch {
      case e: Exception =>
        logger.error(s"Failed to get value")
        throw e
    }
  }
  override def close() = {
    bufferedReader.close()
  }
}
