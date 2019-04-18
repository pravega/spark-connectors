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
                              options: DataSourceOptions) extends MicroBatchReader with Logging {

  private val streamManager = StreamManager.create(clientConfig)
  private val clientFactory = ClientFactory.withScope(scopeName, clientConfig)
  private val batchClient = clientFactory.createBatchClient()
  private var startStreamCut: StreamCut = _
  private var endStreamCut: StreamCut = _

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
    val streamInfo = streamManager.getStreamInfo(scopeName, streamName)

    startStreamCut = Option(start.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(streamInfo.getHeadStreamCut)

    endStreamCut = Option(end.orElse(null))
      .map(_.asInstanceOf[PravegaSourceOffset].streamCut)
      .getOrElse(streamInfo.getTailStreamCut)

    log.info(s"setOffsetRange(${start},${end}): startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    batchClient
      .getSegments(Stream.of(scopeName, streamName), startStreamCut, endStreamCut)
      .getIterator
      .asScala
      .toList
      .map(PravegaMicroBatchInputPartition(_, clientConfig): InputPartition[InternalRow])
      .asJava
  }

  override def getStartOffset: Offset = {
    PravegaSourceOffset(startStreamCut)
  }

  override def getEndOffset: Offset = {
    PravegaSourceOffset(endStreamCut)
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
                                            clientConfig: ClientConfig) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    PravegaMicroBatchInputPartitionReader(segmentRange, clientConfig)
}

/** A [[InputPartitionReader]] for reading Pravega data in a micro-batch streaming query. */
case class PravegaMicroBatchInputPartitionReader(
                                                  segmentRange: SegmentRange,
                                                  clientConfig: ClientConfig) extends InputPartitionReader[InternalRow] with Logging {

  private val clientFactory = ClientFactory.withScope(segmentRange.getScope, clientConfig)
  private val batchClient = clientFactory.createBatchClient()
  private val iterator = batchClient.readSegment(segmentRange, new ByteBufferSerializer)
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

object PravegaReader {
  def pravegaSchema: StructType = StructType(Seq(
    StructField("event", BinaryType),
    StructField("scope", StringType),
    StructField("stream", StringType),
    StructField("segment_id", LongType),
    StructField("offset", LongType)
  ))
}
