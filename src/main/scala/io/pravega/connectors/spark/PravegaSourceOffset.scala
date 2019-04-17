package io.pravega.connectors.spark

import io.pravega.client.stream.StreamCut
import org.apache.spark.sql.sources.v2.reader.streaming.Offset

/**
 * An [[Offset]] for the [[StreamCut]].
 */
case class PravegaSourceOffset(streamCut: StreamCut) extends Offset {

  override val json: String = JsonUtils.streamCut(streamCut)
}
