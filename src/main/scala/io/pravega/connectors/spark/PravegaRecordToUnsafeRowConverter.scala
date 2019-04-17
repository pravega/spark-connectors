package io.pravega.connectors.spark

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.unsafe.types.UTF8String

/** A simple class for converting Pravega events to UnsafeRow */
class PravegaRecordToUnsafeRowConverter {
  private val rowWriter = new UnsafeRowWriter(5)

  def toUnsafeRow(event: Array[Byte], scope: String, streamName: String, segmentId: Long, offset: Long): UnsafeRow = {
    rowWriter.reset()
    rowWriter.write(0, event)
    rowWriter.write(1, UTF8String.fromString(scope))
    rowWriter.write(2, UTF8String.fromString(streamName))
    rowWriter.write(3, segmentId)
    rowWriter.write(4, offset)
    rowWriter.getRow
  }
}
