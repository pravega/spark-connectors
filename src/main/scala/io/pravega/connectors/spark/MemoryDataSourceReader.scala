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

import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

/**
  * A simple DataSourceReader to provide small tables as a dataframe.
  * This is used to provide Pravega stream metadata.
  */
case class MemoryDataSourceReader(
                            schema: StructType,
                            rows: Seq[InternalRow]) extends DataSourceReader with Logging {
  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] =
    Seq(MemoryInputPartition(rows): InputPartition[InternalRow]).asJava

  override def readSchema(): StructType = schema
}

case class MemoryInputPartition(rows: Seq[InternalRow]) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = MemoryInputPartitionReader(rows)
}

case class MemoryInputPartitionReader(rows: Seq[InternalRow]) extends InputPartitionReader[InternalRow] {
  private val it = rows.iterator
  override def next(): Boolean = it.hasNext
  override def get(): InternalRow = it.next
  override def close(): Unit = {}
}
