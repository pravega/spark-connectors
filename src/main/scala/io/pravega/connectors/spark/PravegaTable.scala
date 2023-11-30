package io.pravega.connectors.spark

import java.util
import java.util.Locale

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class PravegaTable extends Table with SupportsWrite with SupportsRead with Logging {

  override def name(): String = "PravegaTable"

  override def schema(): StructType = PravegaReader.pravegaSchema

  override def capabilities(): util.Set[TableCapability] = {
    import TableCapability._
    Set(BATCH_READ, BATCH_WRITE, MICRO_BATCH_READ, STREAMING_WRITE, ACCEPT_ANY_SCHEMA).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    () => new PravegaScan(options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {
      private val inputSchema: StructType = info.schema()
      private val options = info.options();
      private val parameters = options.asScala.toMap
      private val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
      PravegaSourceProvider.validateOptions(caseInsensitiveParams)
      val clientConfig = PravegaSourceProvider.buildClientConfig(caseInsensitiveParams)
      val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
      val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)
      val readAfterWriteConsistency = caseInsensitiveParams.getOrElse(PravegaSourceProvider.READ_AFTER_WRITE_CONSISTENCY_OPTION_KEY, "true").toBoolean
      val transactionStatusPollIntervalMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_STATUS_POLL_INTERVAL_MS_OPTION_KEY) match {
        case Some(s: String) => s.toLong
        case None => PravegaSourceProvider.DEFAULT_TRANSACTION_STATUS_POLL_INTERVAL_MS
      }

      log.info(s"newWriteBuilder: parameters=${parameters}, clientConfig=${clientConfig}")

      PravegaSourceProvider.createStream(caseInsensitiveParams)


      /**
       * Creates an optional {@link BatchWrite} to save the data to this data source. Data
       * sources can return None if there is no writing needed to be done according to the save mode.
       *
       * This is used to write a dataframe to a Pravega stream in a batch job.
       *
       * If this method fails (by throwing an exception), the action will fail and no Spark job will be
       * submitted.
       */
      override def buildForBatch(): BatchWrite = {
        assert(inputSchema != null)
        val transactionTimeoutMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_TIMEOUT_MS_OPTION_KEY) match {
          case Some(s: String) => s.toLong
          case None => PravegaSourceProvider.DEFAULT_BATCH_TRANSACTION_TIMEOUT_MS
        }
        if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.EXACTLY_ONCE, "true").toBoolean) {
          new TransactionPravegaWriter(
            scopeName,
            streamName,
            clientConfig,
            transactionTimeoutMs,
            readAfterWriteConsistency,
            transactionStatusPollIntervalMs,
            inputSchema)
        } else {
          new NonTransactionPravegaWriter(
            scopeName,
            streamName,
            clientConfig,
            inputSchema)
        }

      }

      /**
       * Creates an optional {@link StreamingWrite} to save the data to this data source. Data
       * sources can return None if there is no writing needed to be done.
       * This is used to write a datastream to a Pravega stream in a structured streaming job.
       */
      override def buildForStreaming(): StreamingWrite = {
        assert(inputSchema != null)
        val transactionTimeoutMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_TIMEOUT_MS_OPTION_KEY) match {
          case Some(s: String) => s.toLong
          case None => PravegaSourceProvider.DEFAULT_TRANSACTION_TIMEOUT_MS
        }
        if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.EXACTLY_ONCE, "true").toBoolean) {
          new TransactionPravegaWriter(
            scopeName,
            streamName,
            clientConfig,
            transactionTimeoutMs,
            readAfterWriteConsistency,
            transactionStatusPollIntervalMs,
            inputSchema)
        } else {
          new NonTransactionPravegaWriter(
            scopeName,
            streamName,
            clientConfig,
            inputSchema)
        }
      }

      override def truncate(): WriteBuilder = this
    }
  }
}
