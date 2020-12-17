package io.pravega.connectors.spark

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class PravegaMetaTable(metadataTableName: MetadataTableName.MetadataTableName) extends Table with SupportsRead {
  override def name(): String = "PravegaMataTable"

  override def schema(): StructType = {
    metadataTableName match {
      case MetadataTableName.StreamInfo => {
        StructType(Seq(
          StructField("head_stream_cut", StringType),
          StructField("tail_stream_cut", StringType)
        ))
      }
      case MetadataTableName.Streams => {
        StructType(Seq(
          StructField("scope_name", StringType),
          StructField("stream_name", StringType)
        ))
      }
    }
  }

  override def capabilities(): util.Set[TableCapability] = {
    import TableCapability._
    Set(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, ACCEPT_ANY_SCHEMA).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => new PravegaMetaScan(options, metadataTableName)

  }
}
