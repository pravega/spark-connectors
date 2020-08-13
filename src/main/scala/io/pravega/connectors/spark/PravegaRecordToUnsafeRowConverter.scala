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

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.unsafe.types.UTF8String

/** A simple class for converting Pravega events to UnsafeRow */
class PravegaRecordToUnsafeRowConverter {
  private val rowWriter = new UnsafeRowWriter(5)

  def toUnsafeRow(event: Array[Byte], scope: String, streamName: String, segmentId: Long, offset: Long): UnsafeRow = {
    rowWriter.reset()
    rowWriter.zeroOutNullBytes()
    rowWriter.write(0, event)
    rowWriter.write(1, UTF8String.fromString(scope))
    rowWriter.write(2, UTF8String.fromString(streamName))
    rowWriter.write(3, segmentId)
    rowWriter.write(4, offset)
    rowWriter.getRow
  }
}
