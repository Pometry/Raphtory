package com.raphtory.spouts

import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

/** A [[com.raphtory.api.input.Spout Spout]] that reads files from disk.
  *
  * This Spout takes a url to pull data from, along with authorization and a content type.
  * It is of type String and so pulls in data as a String into Raphtory.
  *
  * @param url The url that the user would like to pull data from, set in the application.conf.
  * @param auth The authorization code in Base64 format to access the url e.g. "Basic XXXXXXXXX"
  * @param contentType The content type of the data e.g. application/json
  *
  * @see [[com.raphtory.api.input.Spout Spout]]
  */
case class WebSocketSpout(url: String, auth: Option[String], contentType: Option[String]) extends Spout[String] {
  override def buildSpout(): SpoutInstance[String] = new WebSocketSpoutInstance(url, auth, contentType)
}

class WebSocketSpoutInstance(url: String, auth: Option[String], contentType: Option[String])
        extends SpoutInstance[String] {
  val client                  = HttpClients.custom().build()
  var request: HttpUriRequest = _
  if (auth.nonEmpty && contentType.nonEmpty)
    request = RequestBuilder
      .get()
      .setUri(url)
      .setHeader(HttpHeaders.AUTHORIZATION, auth.get)
      .addHeader(HttpHeaders.CONTENT_TYPE, contentType.get)
      .build()
  else
    request = RequestBuilder.get().setUri(url).build()

  var input: String         = _
  val execute               = client.execute(request)
  val inStream: InputStream = execute.getEntity.getContent
  val bufferedReader        = new BufferedReader(new InputStreamReader(execute.getEntity.getContent))
  val stringBuilder         = new StringBuilder()

  override def spoutReschedules(): Boolean = true

  override def hasNext: Boolean = bufferedReader.readLine().nonEmpty

  override def next(): String =
    try {
      input = bufferedReader.readLine()
      stringBuilder.append(input)
      input
    }
    catch {
      case e: Exception =>
        logger.error(s"Failed to get value")
        throw e
    }

  override def close() =
    bufferedReader.close()
}
