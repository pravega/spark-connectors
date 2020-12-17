package io.pravega.connectors.spark

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import resource.managed

import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsScalaMapConverter}

class PravegaMetaScan(options: CaseInsensitiveStringMap, metadataTableName: MetadataTableName.MetadataTableName)
  extends Scan with Logging{

  private def createMetadataReader(scopeName: String,
                                   streamName: String,
                                   clientConfig: ClientConfig,
                                   metadataTableName: MetadataTableName.MetadataTableName,
                                   caseInsensitiveParams: Map[String, String]): Batch = {
    (for (streamManager <- managed(StreamManager.create(clientConfig))) yield {
      metadataTableName match {
        case MetadataTableName.StreamInfo => {
          val streamInfo = streamManager.getStreamInfo(scopeName, streamName)
          val schema = StructType(Seq(
            StructField("head_stream_cut", StringType),
            StructField("tail_stream_cut", StringType)
          ))
          val row = new GenericInternalRow(Seq(
            UTF8String.fromString(streamInfo.getHeadStreamCut.asText()),
            UTF8String.fromString(streamInfo.getTailStreamCut.asText())
          ).toArray[Any])
          MemoryDataSourceReader(schema, Seq(row))
        }
        case MetadataTableName.Streams => {
          val streams = streamManager.listStreams(scopeName)
          val schema = StructType(Seq(
            StructField("scope_name", StringType),
            StructField("stream_name", StringType)
          ))

          var rows = Seq[InternalRow]()

          streams.asScala.foreach(stream => {
            rows = rows :+ new GenericInternalRow(
              Seq(
                UTF8String.fromString(stream.getScope),
                UTF8String.fromString(stream.getStreamName)
              ).toArray[Any])
          })

          MemoryDataSourceReader(schema, rows)
        }
      }
    }).acquireAndGet(identity _)
  }

  override def readSchema(): StructType = {
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

  override def toBatch: Batch = {
    val caseInsensitiveParams = CaseInsensitiveMap(options.asScala.toMap)
    PravegaSourceProvider.validateOptions(caseInsensitiveParams)
    val clientConfig = PravegaSourceProvider.buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)

    val metadataTableName = caseInsensitiveParams.get(PravegaSourceProvider.METADATA_OPTION_KEY).map(MetadataTableName.withName)

    createMetadataReader(
      scopeName,
      streamName,
      clientConfig,
      metadataTableName.get,
      caseInsensitiveParams)
  }
}
