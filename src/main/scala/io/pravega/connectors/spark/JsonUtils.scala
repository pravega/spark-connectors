package io.pravega.connectors.spark

import io.pravega.client.stream.StreamCut
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Utilities for converting Pravega related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Read stream cut from json string
   */
  def streamCut(str: String): StreamCut = {
      StreamCut.from(Serialization.read[String](str))
  }

  /**
   * Write stream cut as json string
   */
  def streamCut(streamCut: StreamCut): String = {
    Serialization.write(streamCut.asText())
  }
}
