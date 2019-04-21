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

import org.apache.spark.sql.Row

/** A simple class for converting Pravega events to UnsafeRow */
class PravegaRecordToRowConverter {
  def toRow(event: Array[Byte], scope: String, streamName: String, segmentId: Long, offset: Long): Row = {
    Row(
      event,
      scope,
      streamName,
      segmentId,
      offset
    )
  }
}
