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

import io.pravega.client.ClientConfig
import io.pravega.client.stream.StreamCut
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader

/**
  * A [[DataSourceReader]] for Pravega. It uses the Pravega batch API to read from a Pravega stream.
  * This is used to read a Pravega stream as a dataframe in a batch job.
  */
class PravegaDataSourceReader(scopeName: String,
                              streamName: String,
                              clientConfig: ClientConfig,
                              encoding: Encoding.Value,
                              options: DataSourceOptions,
                              startStreamCut: PravegaStreamCut,
                              endStreamCut: PravegaStreamCut
   ) extends PravegaReaderBase(scopeName, streamName, clientConfig, encoding, options)
     with Logging {

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
}
