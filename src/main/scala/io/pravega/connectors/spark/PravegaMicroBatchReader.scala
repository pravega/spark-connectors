/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.spark

import java.io.IOException
import java.{util => ju}

import io.pravega.client.admin.StreamManager
import io.pravega.client.batch.SegmentRange
import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{Stream, StreamCut}
import io.pravega.client.{ClientConfig, ClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
  * A [[MicroBatchReader]] for Pravega. It uses the Pravega batch API to read micro batches
  * from a Pravega stream.
  *
  * @param scopeName
  * @param streamName
  * @param clientConfig
  * @param options
  */
class PravegaMicroBatchReader(scopeName: String,
                              streamName: String,
                              clientConfig: ClientConfig,
                              encoding: Encoding.Value,
                              options: DataSourceOptions,
                              startStreamCut: PravegaStreamCut,
                              endStreamCut: PravegaStreamCut) extends MicroBatchReader with Logging {

  private val streamManager = StreamManager.create(clientConfig)
  private val clientFactory = ClientFactory.withScope(scopeName, clientConfig)
  private val batchClient = clientFactory.createBatchClient()
  private var batchStartStreamCut: StreamCut = _
  private var batchEndStreamCut: StreamCut = _

  // Resolve start and end stream cuts now.
  private lazy val initialStreamInfo = streamManager.getStreamInfo(scopeName, streamName)
  private val resolvedStartStreamCut = startStreamCut match {
    case EarliestStreamCut | UnboundedStreamCut => initialStreamInfo.getHeadStreamCut
    case LatestStreamCut => initialStreamInfo.getTailStreamCut
    case SpecificStreamCut(sc) => sc
  }
  private val resolvedEndStreamCut = endStreamCut match {
    case EarliestStreamCut => Some(initialStreamInfo.getHeadStreamCut)
    case LatestStreamCut => Some(initialStreamInfo.getTailStreamCut)
    case SpecificStreamCut(sc) => Some(sc)
    case UnboundedStreamCut => None
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

    lazy val streamInfo = streamManager.getStreamInfo(scopeName, streamName)

    batchStartStreamCut = Option(start.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedStartStreamCut)

    batchEndStreamCut = Option(end.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(resolvedEndStreamCut.getOrElse(streamInfo.getTailStreamCut))

    log.info(s"setOffsetRange(${start},${end}): batchStartStreamCut=${batchStartStreamCut}, batchEndStreamCut=${batchEndStreamCut}")
  }

  /**
    * Returns a list of {@link InputPartition}s. Each {@link InputPartition} is responsible for
    * creating a data reader to output data of one RDD partition. The number of input partitions
    * returned here is the same as the number of RDD partitions this scan outputs.
    *
    * Note that, this may not be a full scan if the data source reader mixes in other optimization
    * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
    * Spark issues the scan request.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be
    * submitted.
    */
  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    log.info("planInputPartitions")
    batchClient
      .getSegments(Stream.of(scopeName, streamName), batchStartStreamCut, batchEndStreamCut)
      .getIterator
      .asScala
      .toList
      .map(PravegaMicroBatchInputPartition(_, clientConfig, encoding): InputPartition[InternalRow])
      .asJava
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

  override def readSchema(): StructType = PravegaReader.pravegaSchema

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    clientFactory.close()
    streamManager.close()
  }

  override def toString: String = s"Pravega"
}

/** A [[InputPartition]] for reading Pravega data in a micro-batch streaming query. */
case class PravegaMicroBatchInputPartition(
                                            segmentRange: SegmentRange,
                                            clientConfig: ClientConfig,
                                            encoding: Encoding.Value) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    PravegaMicroBatchInputPartitionReader(segmentRange, clientConfig, encoding)
}

/** A [[InputPartitionReader]] for reading Pravega data in a micro-batch streaming query. */
case class PravegaMicroBatchInputPartitionReader(
                                                  segmentRange: SegmentRange,
                                                  clientConfig: ClientConfig,
                                                  encoding: Encoding.Value) extends InputPartitionReader[InternalRow] with Logging {

  private val clientFactory = ClientFactory.withScope(segmentRange.getScope, clientConfig)
  private val batchClient = clientFactory.createBatchClient()
  private val rawIterator = batchClient.readSegment(segmentRange, new ByteBufferSerializer)
  private val iterator = EventIterator(rawIterator, encoding)

  private val converter = new PravegaRecordToUnsafeRowConverter
  private var nextRow: UnsafeRow = _

  /**
    * Proceed to next event, returns false if there is no more records.
    *
    * If this method fails (by throwing an exception), the corresponding Spark task would fail and
    * get retried until hitting the maximum retry times.
    *
    * @throws IOException if failure happens during disk/network IO like reading files.
    */
  override def next(): Boolean = {
    if (iterator.hasNext) {
      val offset = iterator.getOffset
      val event = iterator.next()
      nextRow = converter.toUnsafeRow(
        event.array,
        segmentRange.getScope,
        segmentRange.getStreamName,
        segmentRange.getSegmentId,
        offset)
      true
    } else false
  }

  /**
    * Return the current event. This method should return same value until `next` is called.
    *
    * If this method fails (by throwing an exception), the corresponding Spark task would fail and
    * get retried until hitting the maximum retry times.
    */
  override def get(): UnsafeRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit = {
    iterator.close()
    clientFactory.close()
  }
}
