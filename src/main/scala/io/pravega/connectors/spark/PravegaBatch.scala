/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.spark

import io.pravega.client.stream.{Stream, StreamCut}
import io.pravega.client.{BatchClientFactory, ClientConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import resource.managed

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * A [[Batch]] for Pravega. It uses the Pravega batch API to read from a Pravega stream.
 * This is used to read a Pravega stream as a dataframe in a batch job.
 */
class PravegaBatch(
                    scopeName: String,
                    streamName: String,
                    clientConfig: ClientConfig,
                    options: CaseInsensitiveMap[String],
                    startStreamCut: PravegaStreamCut,
                    endStreamCut: PravegaStreamCut
                  )
  extends Batch with Logging {
  protected var batchStartStreamCut: StreamCut = _
  protected var batchEndStreamCut: StreamCut = _

  batchStartStreamCut = startStreamCut match {
    case EarliestStreamCut | UnboundedStreamCut => StreamCut.UNBOUNDED
    case SpecificStreamCut(sc) => sc
    case _ => throw new IllegalArgumentException()
  }
  batchEndStreamCut = endStreamCut match {
    case LatestStreamCut | UnboundedStreamCut => StreamCut.UNBOUNDED
    case SpecificStreamCut(sc) => sc
    case _ => throw new IllegalArgumentException()
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
  override def planInputPartitions(): Array[InputPartition] = {
    (for (batchClientFactory <- managed(BatchClientFactory.withScope(scopeName, clientConfig))) yield {
      batchClientFactory
        .getSegments(Stream.of(scopeName, streamName), batchStartStreamCut, batchEndStreamCut)
        .getIterator
        .asScala
        .toList
        .map(PravegaBatchInputPartition(_, clientConfig): InputPartition)
        .toArray
    }).acquireAndGet(identity)

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    PravegaBatchReaderFactory
  }

  override def toString: String = {
    s"PravegaBatch{clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}" +
      s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}}"
  }
}
