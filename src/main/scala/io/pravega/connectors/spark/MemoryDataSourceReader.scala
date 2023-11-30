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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types._

/**
  * A simple DataSourceReader to provide small tables as a dataframe.
  * This is used to provide Pravega stream metadata.
  */
case class MemoryDataSourceReader(
                                   schema: StructType,
                                   rows: Seq[InternalRow]) extends Scan with Batch with Logging {
  override def planInputPartitions():  Array[InputPartition] =
    Array(SimpleInputPartition(rows))

  override def readSchema(): StructType = schema

  override def createReaderFactory(): PartitionReaderFactory = MemoryInputPartition(rows)
}

case class SimpleInputPartition(rows: Seq[InternalRow]) extends InputPartition

case class MemoryInputPartition(rows: Seq[InternalRow]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    MemoryInputPartitionReader(partition.asInstanceOf[SimpleInputPartition])
  }
}

case class MemoryInputPartitionReader(inputPartition: SimpleInputPartition) extends PartitionReader[InternalRow] {
  private val rows = inputPartition.rows.toArray
  private val it = rows.iterator
  override def next(): Boolean = it.hasNext
  override def get(): InternalRow = it.next
  override def close(): Unit = {}
}
