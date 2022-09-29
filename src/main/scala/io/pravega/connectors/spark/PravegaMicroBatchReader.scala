/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.spark

import java.{util => ju}

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}

/**
  * A [[MicroBatchReader]] for Pravega. It uses the Pravega batch API to read micro batches
  * from a Pravega stream.
  *
  * @param startStreamCut   Not used when starting from a checkpoint.
  * @param endStreamCut     Not used when starting from a checkpoint.
  */
class PravegaMicroBatchReader(scopeName: String,
                              streamName: String,
                              clientConfig: ClientConfig,
                              options: DataSourceOptions,
                              startStreamCut: PravegaStreamCut,
                              endStreamCut: PravegaStreamCut
    ) extends PravegaReaderBase(scopeName, streamName, clientConfig, options)
      with MicroBatchReader with Logging with AutoCloseable {

  private val streamManager = StreamManager.create(clientConfig)

  // Resolve start and end stream cuts now.
  // We must ensure that getStartOffset and getEndOffset return specific stream cuts, even
  // if the caller passes "earliest" or "latest".
  private lazy val initialStreamInfo = PravegaUtils.getStreamInfo(streamManager, scopeName, streamName)
  private val resolvedStartStreamCut = startStreamCut match {
    case EarliestStreamCut | UnboundedStreamCut => initialStreamInfo.getHeadStreamCut
    case LatestStreamCut => initialStreamInfo.getTailStreamCut
    case SpecificStreamCut(sc) => sc
  }
  private val resolvedEndStreamCut = endStreamCut match {
    case LatestStreamCut => Some(initialStreamInfo.getTailStreamCut)
    case SpecificStreamCut(sc) => Some(sc)
    case UnboundedStreamCut => None
    case _ => throw new IllegalArgumentException()
  }
  log.info(s"resolvedStartStreamCut=${resolvedStartStreamCut}, resolvedEndStreamCut=${resolvedEndStreamCut}")

  /**
    * Set the desired offset range for input partitions created from this reader. Partition readers
    * will generate only data within (`start`, `end`]; that is, from the first record after `start`
    * to the record with offset `end`.
    *
    * @param start The initial offset to scan from. If not specified, scan from an
    *              implementation-specified start point, such as the earliest available record.
    * @param end   The last offset to include in the scan. If not specified, scan up to an
    *              implementation-defined endpoint, such as the last available offset
    *              or the start offset plus a target batch size.
    */
  override def setOffsetRange(start: ju.Optional[Offset], end: ju.Optional[Offset]): Unit = {
    log.info(s"setOffsetRange(${start},${end})")

    lazy val streamInfo = PravegaUtils.getStreamInfo(streamManager, scopeName, streamName)

    // The batch will start at the first available stream cut: start, resolvedStartStreamCut
    batchStartStreamCut = Option(start.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedStartStreamCut)

    // The batch will end at the first available stream cut: end, resolvedEndStreamCut, getTailStreamCut
    batchEndStreamCut = Option(end.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedEndStreamCut.getOrElse(streamInfo.getTailStreamCut))

    log.info(s"setOffsetRange(${start},${end}): batchStartStreamCut=${batchStartStreamCut}, batchEndStreamCut=${batchEndStreamCut}")
  }

  override def getStartOffset: Offset = {
    PravegaSourceOffset(batchStartStreamCut)
  }

  override def getEndOffset: Offset = {
    PravegaSourceOffset(batchEndStreamCut)
  }

  override def deserializeOffset(json: String): Offset = {
    PravegaSourceOffset(JsonUtils.streamCut(json))
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    close()
  }

  override def close(): Unit = {
    streamManager.close()
  }
}
