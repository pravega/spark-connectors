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

import io.pravega.client.batch.SegmentRange
import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{Stream, StreamCut}
import io.pravega.client.{BatchClientFactory, ClientConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._
import resource.managed

import scala.collection.JavaConverters._

/**
  * A base class for both the batch and micro-batch Pravega readers.
  */
class PravegaReaderBase(
                          scopeName: String,
                          streamName: String,
                          clientConfig: ClientConfig,
                          encoding: Encoding.Value,
                          options: DataSourceOptions
    ) extends DataSourceReader with Logging {

  protected var batchStartStreamCut: StreamCut = _
  protected var batchEndStreamCut: StreamCut = _

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
    (for (batchClientFactory <- managed(BatchClientFactory.withScope(scopeName, clientConfig))) yield {
      batchClientFactory
        .getSegments(Stream.of(scopeName, streamName), batchStartStreamCut, batchEndStreamCut)
        .getIterator
        .asScala
        .toList
        .map(PravegaInputPartition(_, clientConfig, encoding): InputPartition[InternalRow])
        .asJava
    }).acquireAndGet(identity)
  }

  override def readSchema(): StructType = PravegaReader.pravegaSchema

  override def toString: String = PravegaSourceProvider.SOURCE_PROVIDER_NAME
}

/** An [[InputPartition]] for reading a Pravega stream. */
case class PravegaInputPartition(
                                  segmentRange: SegmentRange,
                                  clientConfig: ClientConfig,
                                  encoding: Encoding.Value) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    PravegaInputPartitionReader(segmentRange, clientConfig, encoding)
}

/** An [[InputPartitionReader]] for reading a Pravega stream. */
case class PravegaInputPartitionReader(
                                        segmentRange: SegmentRange,
                                        clientConfig: ClientConfig,
                                        encoding: Encoding.Value) extends InputPartitionReader[InternalRow] with Logging {

  private val batchClientFactory = BatchClientFactory.withScope(segmentRange.getScope, clientConfig)
  private val rawIterator = batchClientFactory.readSegment(segmentRange, new ByteBufferSerializer)
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
    log.debug("close: BEGIN")
    iterator.close()
    batchClientFactory.close()
    log.debug("close: END")
  }
}
