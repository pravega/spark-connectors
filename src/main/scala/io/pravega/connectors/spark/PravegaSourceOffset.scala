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

import io.pravega.client.stream.StreamCut
import org.apache.spark.sql.sources.v2.reader.streaming.Offset

/**
 * An [[Offset]] for the [[StreamCut]].
 */
case class PravegaSourceOffset(streamCut: StreamCut) extends Offset {

  override val json: String = JsonUtils.streamCut(streamCut)
}
