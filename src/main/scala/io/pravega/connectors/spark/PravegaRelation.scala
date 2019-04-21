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

import io.pravega.client.admin.StreamManager
import io.pravega.client.batch.SegmentRange
import io.pravega.client.stream.Stream
import io.pravega.client.{ClientConfig, ClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import resource.managed
import scala.collection.JavaConverters._

class PravegaRelation(
    override val sqlContext: SQLContext,
    parameters: Map[String, String],
    scopeName: String,
    streamName: String,
    clientConfig: ClientConfig,
    encoding: Encoding.Value)
    extends BaseRelation with TableScan with Logging {

  override def schema: StructType = PravegaReader.pravegaSchema

  override def buildScan(): RDD[Row] = {
    val segmentRanges = (for {
      streamManager <- managed(StreamManager.create(clientConfig))
      clientFactory <- managed(ClientFactory.withScope(scopeName, clientConfig))
    } yield {
    val batchClient = clientFactory.createBatchClient()

    val streamInfo = streamManager.getStreamInfo(scopeName, streamName)
    val startStreamCut = streamInfo.getHeadStreamCut
    val endStreamCut = streamInfo.getTailStreamCut

    log.info(s"buildScan: startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")

    val segmentRanges: Seq[SegmentRange] = batchClient
      .getSegments(Stream.of(scopeName, streamName), startStreamCut, endStreamCut)
      .getIterator
      .asScala
      .toList

    log.info(s"buildScan: segmentRanges=${segmentRanges}")

    segmentRanges
    }).acquireAndGet(identity)

    // Create an RDD that reads from Pravega.
    new PravegaSourceRDD(
      sqlContext.sparkContext,
      segmentRanges,
      clientConfig,
      encoding)
  }

  override def toString: String =
    s"PravegaRelation(scopeName=$scopeName, streamName=$streamName)"
}
