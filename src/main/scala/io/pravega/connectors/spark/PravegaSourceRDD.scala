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

import io.pravega.client.batch.SegmentRange
import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.{ClientConfig, ClientFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

/** Partition of the PravegaSourceRDD */
case class PravegaSourceRDDPartition(
  index: Int,
  segmentRange: SegmentRange) extends Partition

/**
 * An RDD that reads data from Pravega.
 *
 * @param sc the [[SparkContext]]
 * @param segmentRanges Offset ranges that define the Pravega data belonging to this RDD
 */
class PravegaSourceRDD(
    sc: SparkContext,
    segmentRanges: Seq[SegmentRange],
    clientConfig: ClientConfig,
    encoding: Encoding.Value)
  extends RDD[Row](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    segmentRanges.zipWithIndex.map { case (o, i) => PravegaSourceRDDPartition(i, o) }.toArray
  }

  /**
    * Compute a given partition.
    */
  override def compute(
      thePart: Partition,
      context: TaskContext): Iterator[Row] = {

    val sourcePartition = thePart.asInstanceOf[PravegaSourceRDDPartition]
    val segmentRange = sourcePartition.segmentRange
    val clientFactory = ClientFactory.withScope(segmentRange.getScope, clientConfig)
    val batchClient = clientFactory.createBatchClient()

    context.addTaskCompletionListener[Unit] { _ =>
      log.info("compute: closing clientFactory")
      clientFactory.close()
    }

    val rawIterator = batchClient.readSegment(segmentRange, new ByteBufferSerializer)
    val iterator = EventIterator(rawIterator, encoding)

    context.addTaskCompletionListener[Unit] { _ =>
      log.info("compute: closing iterator")
      iterator.close()
    }

    val converter = new PravegaRecordToRowConverter

    val rowIterator: Iterator[Row] = new Iterator[Row]() {
      override def hasNext(): Boolean = {
        iterator.hasNext
      }

      override def next(): Row = {
        val offset = iterator.getOffset
        val event = iterator.next
        converter.toRow(
          event.array,
          segmentRange.getScope,
          segmentRange.getStreamName,
          segmentRange.getSegmentId,
          offset)
      }
    }

    rowIterator
  }
}
