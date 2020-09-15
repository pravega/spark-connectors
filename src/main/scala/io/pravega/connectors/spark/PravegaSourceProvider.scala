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

import java.net.URI
import java.time.Duration
import java.util.{Locale, Optional}

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{RetentionPolicy, ScalingPolicy, StreamConfiguration, StreamCut}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.unsafe.types.UTF8String
import resource.managed

import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConverters._
import scala.util.Try

object MetadataTableName extends Enumeration {
  type MetadataTableName = Value
  val StreamInfo: Value = Value("StreamInfo")
  val Streams: Value = Value("Streams")
}

class PravegaSourceProvider extends DataSourceV2
  with MicroBatchReadSupport
  with ReadSupport
  with DataSourceRegister
  with StreamWriteSupport
  with WriteSupport
  with Logging {

  private val DEFAULT_CONTROLLER = "tcp://localhost:9090"
  private val DEFAULT_TRANSACTION_TIMEOUT_MS: Long = 30 * 1000
  private val DEFAULT_BATCH_TRANSACTION_TIMEOUT_MS: Long = 2 * 60 * 1000 // 2 minutes (maximum allowed by default server)
  private val DEFAULT_TRANSACTION_STATUS_POLL_INTERVAL_MS: Long = 50

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = PravegaSourceProvider.SOURCE_PROVIDER_NAME

  /**
    * Creates a {@link MicroBatchReader} to read batches of data from this data source in a
    * streaming query.
    * This is used to read a Pravega stream as a datastream in a structured streaming job.
    *
    * The execution engine will create a micro-batch reader at the start of a streaming query,
    * alternate calls to setOffsetRange and planInputPartitions for each batch to process, and
    * then call stop() when the execution is complete. Note that a single query may have multiple
    * executions due to restart or failure recovery.
    *
    * @param schema             the user provided schema, or empty() if none was provided
    * @param checkpointLocation a path to Hadoop FS scratch space that can be used for failure
    *                           recovery. Readers for the same logical source in the same query
    *                           will be given the same checkpointLocation.
    * @param options            the options for the returned data source reader, which is an immutable
    *                           case-insensitive string-to-string map.
    */
  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {

    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateStreamOptions(caseInsensitiveParams)

    val clientConfig = buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)

    val startStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.START_STREAM_CUT_OPTION_KEY, LatestStreamCut)

    val endStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.END_STREAM_CUT_OPTION_KEY, UnboundedStreamCut)

    log.info(s"createMicroBatchReader: clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}"
      + s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")

    createStreams(caseInsensitiveParams)

    new PravegaMicroBatchReader(
      scopeName,
      streamName,
      clientConfig,
      options,
      startStreamCut,
      endStreamCut)
  }

  /**
    * Creates a {@link DataSourceReader} to scan the data from this data source.
    *
    * This is used to read a Pravega stream as a dataframe in a batch job.
    *
    * Unbounded stream cuts (earliest, latest, unbounded) are bound only once.
    * Late binding is not available.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be
    * submitted.
    *
    * @param schema  the user specified schema.
    * @param options the options for the returned data source reader, which is an immutable
    *                case-insensitive string-to-string map.
    */
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    validateBatchOptions(caseInsensitiveParams)
    val clientConfig = buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)

    val metadataTableName = caseInsensitiveParams.get(PravegaSourceProvider.METADATA_OPTION_KEY).map(MetadataTableName.withName)
    if (metadataTableName.isDefined) {
      return createMetadataReader(
        scopeName,
        streamName,
        clientConfig,
        metadataTableName.get,
        caseInsensitiveParams)
    }

    val startStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.START_STREAM_CUT_OPTION_KEY, EarliestStreamCut)

    val endStreamCut = PravegaSourceProvider.getPravegaStreamCut(
      caseInsensitiveParams, PravegaSourceProvider.END_STREAM_CUT_OPTION_KEY, LatestStreamCut)

    log.info(s"createReader: clientConfig=${clientConfig}, scopeName=${scopeName}, streamName=${streamName}"
      + s" startStreamCut=${startStreamCut}, endStreamCut=${endStreamCut}")

    createStreams(caseInsensitiveParams)

    new PravegaDataSourceReader(
      scopeName,
      streamName,
      clientConfig,
      options,
      startStreamCut,
      endStreamCut)
  }

  private def createMetadataReader(scopeName: String,
                                   streamName: String,
                                   clientConfig: ClientConfig,
                                   metadataTableName: MetadataTableName.MetadataTableName,
                                   caseInsensitiveParams: Map[String, String]): DataSourceReader = {
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

          streams.foreach(stream => {
            rows = rows :+ new GenericInternalRow(
              Seq(
                UTF8String.fromString(stream.getScope),
                UTF8String.fromString(stream.getStreamName)
              ).toArray[Any])
          })

          MemoryDataSourceReader(schema, rows)
        }
      }
    }).acquireAndGet(identity)
  }

  /**
    * Creates an optional {@link StreamWriter} to save the data to this data source. Data
    * sources can return None if there is no writing needed to be done.
    * This is used to write a datastream to a Pravega stream in a structured streaming job.
    *
    * @param queryId A unique string for the writing query. It's possible that there are many
    *                writing queries running at the same time, and the returned
    *                { @link DataSourceWriter} can use this id to distinguish itself from others.
    * @param schema  the schema of the data to be written.
    * @param mode    the output mode which determines what successive epoch output means to this
    *                sink, please refer to { @link OutputMode} for more details.
    * @param options the options for the returned data source writer, which is an immutable
    *                case-insensitive string-to-string map.
    */
  override def createStreamWriter(
                                   queryId: String,
                                   schema: StructType,
                                   mode: OutputMode,
                                   options: DataSourceOptions): StreamWriter = {

    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateStreamOptions(caseInsensitiveParams)

    val clientConfig = buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)
    val transactionTimeoutMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_TIMEOUT_MS_OPTION_KEY) match {
      case Some(s: String) => s.toLong
      case None => DEFAULT_TRANSACTION_TIMEOUT_MS
    }
    val readAfterWriteConsistency = caseInsensitiveParams.getOrElse(PravegaSourceProvider.READ_AFTER_WRITE_CONSISTENCY_OPTION_KEY, "true").toBoolean
    val transactionStatusPollIntervalMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_STATUS_POLL_INTERVAL_MS_OPTION_KEY) match {
      case Some(s: String) => s.toLong
      case None => DEFAULT_TRANSACTION_STATUS_POLL_INTERVAL_MS
    }

    log.info(s"createStreamWriter: parameters=${parameters}, clientConfig=${clientConfig}")

    createStreams(caseInsensitiveParams)

    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.EXACTLY_ONCE, "true").toBoolean) {
      new TransactionPravegaWriter(
        scopeName,
        streamName,
        clientConfig,
        transactionTimeoutMs,
        readAfterWriteConsistency,
        transactionStatusPollIntervalMs,
        schema)
    } else {
      new NonTransactionPravegaWriter(
            scopeName,
            streamName,
            clientConfig,
            schema)
    }

}

  /**
    * Creates an optional {@link DataSourceWriter} to save the data to this data source. Data
    * sources can return None if there is no writing needed to be done according to the save mode.
    *
    * This is used to write a dataframe to a Pravega stream in a batch job.
    *
    * If this method fails (by throwing an exception), the action will fail and no Spark job will be
    * submitted.
    *
    * @param writeUUID A unique string for the writing job. It's possible that there are many writing
    *                  jobs running at the same time, and the returned { @link DataSourceWriter} can
    *                  use this job id to distinguish itself from other jobs.
    * @param schema    the schema of the data to be written.
    * @param mode      the save mode which determines what to do when the data are already in this data
    *                  source, please refer to { @link SaveMode} for more details.
    * @param options   the options for the returned data source writer, which is an immutable
    *                  case-insensitive string-to-string map.
    * @return a writer to append data to this data source
    */
  override def createWriter(
                             writeUUID: String,
                             schema: StructType,
                             mode: SaveMode,
                             options: DataSourceOptions): Optional[DataSourceWriter] = {

    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    validateBatchOptions(caseInsensitiveParams)

    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new IllegalArgumentException(s"Save mode $mode not allowed for Pravega. " +
          s"Allowed save modes are ${SaveMode.Append} and " +
          s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }

    val clientConfig = buildClientConfig(caseInsensitiveParams)
    val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
    val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)
    val transactionTimeoutMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_TIMEOUT_MS_OPTION_KEY) match {
      case Some(s: String) => s.toLong
      case None => DEFAULT_BATCH_TRANSACTION_TIMEOUT_MS
    }
    val readAfterWriteConsistency = caseInsensitiveParams.getOrElse(PravegaSourceProvider.READ_AFTER_WRITE_CONSISTENCY_OPTION_KEY, "true").toBoolean
    val transactionStatusPollIntervalMs = caseInsensitiveParams.get(PravegaSourceProvider.TRANSACTION_STATUS_POLL_INTERVAL_MS_OPTION_KEY) match {
      case Some(s: String) => s.toLong
      case None => DEFAULT_TRANSACTION_STATUS_POLL_INTERVAL_MS
    }

    log.info(s"createWriter: parameters=${parameters}, clientConfig=${clientConfig}")

    createStreams(caseInsensitiveParams)

    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.EXACTLY_ONCE, "true").toBoolean) {
      Optional.of(new TransactionPravegaWriter(
        scopeName,
        streamName,
        clientConfig,
        transactionTimeoutMs,
        readAfterWriteConsistency,
        transactionStatusPollIntervalMs,
        schema))
    } else {
      Optional.of(new NonTransactionPravegaWriter(
        scopeName,
        streamName,
        clientConfig,
        schema))
    }
  }

  def validateStreamOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    validateGeneralOptions(caseInsensitiveParams)

    if (caseInsensitiveParams.contains(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_BYTES_PER_SEC_OPTION_KEY) &&
      caseInsensitiveParams.contains(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_EVENTS_PER_SEC_OPTION_KEY)) {
      throw new IllegalArgumentException(s"Cannot set multiple options for scaling")
    }

    if (caseInsensitiveParams.contains(PravegaSourceProvider.DEFAULT_RETENTION_DURATION_MILLISECONDS_OPTION_KEY) &&
      caseInsensitiveParams.contains(PravegaSourceProvider.DEFAULT_RETENTION_SIZE_BYTES_OPTION_KEY)) {
      throw new IllegalArgumentException(s"Cannot have multiple retention policy options")
    }

    val numSegments = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_NUM_SEGMENTS_OPTION_KEY)
    if (numSegments.isDefined && (Try(numSegments.get.toInt).isFailure || numSegments.get.toInt < 1)) {
      throw new IllegalArgumentException(s"Number of segments needs to be integer and should be at least one")
    }

    val scaleFactor = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SCALE_FACTOR_OPTION_KEY)
    if (scaleFactor.isDefined && (Try(scaleFactor.get.toInt).isFailure || scaleFactor.get.toInt < 2)) {
      throw new IllegalArgumentException(s"Scale factor needs to be an integer greater than 1")
    }

    val targetRateBytesPerSec = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_BYTES_PER_SEC_OPTION_KEY)
    if (targetRateBytesPerSec.isDefined && (Try(targetRateBytesPerSec.get.toInt).isFailure || targetRateBytesPerSec.get.toInt < 1024)) {
      throw new IllegalArgumentException(s"Target rate should an integer and should at least be 1024 bytes")
    }

    val targetRateEventsPerSec = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_EVENTS_PER_SEC_OPTION_KEY)
    if (targetRateEventsPerSec.isDefined && (Try(targetRateEventsPerSec.get.toInt).isFailure || targetRateEventsPerSec.get.toInt < 1)) {
      throw new IllegalArgumentException(s"Target rate should be an integer amd should be at least one event per second")
    }

    val retentionBytes = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_RETENTION_SIZE_BYTES_OPTION_KEY)
    if (retentionBytes.isDefined && (Try(retentionBytes.get.toInt).isFailure || retentionBytes.get.toInt < 0)) {
      throw new IllegalArgumentException(s"Retention size should be an integer more than or equal to zero bytes")
    }

    val retentionMilliseconds = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_RETENTION_DURATION_MILLISECONDS_OPTION_KEY)
    if (retentionMilliseconds.isDefined && (Try(retentionMilliseconds.get.toInt).isFailure || retentionMilliseconds.get.toInt < 0)) {
      throw new IllegalArgumentException(s"Retention time should be an integer morethan or equal to zero milliseconds")
    }
  }

  private def validateBatchOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    // TODO: validate options
    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateGeneralOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.SCOPE_OPTION_KEY, "").isEmpty) {
      throw new IllegalArgumentException(s"Missing required option '${PravegaSourceProvider.SCOPE_OPTION_KEY}'")
    }
    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.STREAM_OPTION_KEY, "").isEmpty) {
      throw new IllegalArgumentException(s"Missing required option '${PravegaSourceProvider.STREAM_OPTION_KEY}'")
    }
  }

  private def buildClientConfig(caseInsensitiveParams: Map[String, String]): ClientConfig = {
    val controllerURI = URI.create(caseInsensitiveParams.getOrElse(PravegaSourceProvider.CONTROLLER_OPTION_KEY, DEFAULT_CONTROLLER))
    ClientConfig.builder()
      .controllerURI(controllerURI)
      .build()
  }

  private def createStreams(caseInsensitiveParams: Map[String, String]): Unit = {
    val clientConfig = buildClientConfig(caseInsensitiveParams)
    for (streamManager <- managed(StreamManager.create(clientConfig))) {
      val allowCreateScope = caseInsensitiveParams.getOrElse(PravegaSourceProvider.ALLOW_CREATE_SCOPE_OPTION_KEY, "true").toBoolean
      val scopeName = caseInsensitiveParams(PravegaSourceProvider.SCOPE_OPTION_KEY)
      if (allowCreateScope) streamManager.createScope(scopeName)

      val streamName = caseInsensitiveParams(PravegaSourceProvider.STREAM_OPTION_KEY)
      val allowCreateStream = caseInsensitiveParams.getOrElse(PravegaSourceProvider.ALLOW_CREATE_STREAM_OPTION_KEY, "true").toBoolean
      if (allowCreateStream) {
        val streamConfig = PravegaSourceProvider.buildStreamConfig(caseInsensitiveParams)
        streamManager.createStream(scopeName, streamName, streamConfig)
      }
    }
  }
}

object PravegaSourceProvider extends Logging {
  private[spark] val SOURCE_PROVIDER_NAME = "pravega"
  private[spark] val CONTROLLER_OPTION_KEY = "controller"
  private[spark] val SCOPE_OPTION_KEY = "scope"
  private[spark] val STREAM_OPTION_KEY = "stream"
  private[spark] val TRANSACTION_TIMEOUT_MS_OPTION_KEY = "transaction_timeout_ms"
  private[spark] val START_STREAM_CUT_OPTION_KEY = "start_stream_cut"
  private[spark] val END_STREAM_CUT_OPTION_KEY = "end_stream_cut"
  private[spark] val ALLOW_CREATE_SCOPE_OPTION_KEY = "allow_create_scope"
  private[spark] val ALLOW_CREATE_STREAM_OPTION_KEY = "allow_create_stream"
  private[spark] val DEFAULT_NUM_SEGMENTS_OPTION_KEY = "default_num_segments"
  private[spark] val READ_AFTER_WRITE_CONSISTENCY_OPTION_KEY = "read_after_write_consistency"
  private[spark] val TRANSACTION_STATUS_POLL_INTERVAL_MS_OPTION_KEY = "transaction_status_poll_interval_ms"
  private[spark] val METADATA_OPTION_KEY = "metadata"
  private[spark] val DEFAULT_SCALE_FACTOR_OPTION_KEY = "default_scale_factor"
  private[spark] val DEFAULT_SEGMENT_TARGET_RATE_BYTES_PER_SEC_OPTION_KEY = "default_segment_target_rate_bytes_per_sec"
  private[spark] val DEFAULT_SEGMENT_TARGET_RATE_EVENTS_PER_SEC_OPTION_KEY = "default_segment_target_rate_events_per_sec"
  private[spark] val DEFAULT_RETENTION_DURATION_MILLISECONDS_OPTION_KEY = "default_retention_duration_milliseconds"
  private[spark] val DEFAULT_RETENTION_SIZE_BYTES_OPTION_KEY = "default_retention_size_bytes"
  private[spark] val EXACTLY_ONCE = "exactly_once"

  private[spark] val STREAM_CUT_EARLIEST = "earliest"
  private[spark] val STREAM_CUT_LATEST = "latest"
  private[spark] val STREAM_CUT_UNBOUNDED = "unbounded"
  private[spark] val ROUTING_KEY_ATTRIBUTE_NAME = "routing_key"
  private[spark] val EVENT_ATTRIBUTE_NAME = "event"


  def buildStreamConfig(caseInsensitiveParams: Map[String, String]): StreamConfiguration = {
    var streamConfig = StreamConfiguration.builder
    val minSegments = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_NUM_SEGMENTS_OPTION_KEY)
    val scaleFactor = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SCALE_FACTOR_OPTION_KEY)
    val targetRateBytesPerSec = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_BYTES_PER_SEC_OPTION_KEY)
    val targetRateEventsPerSec = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_SEGMENT_TARGET_RATE_EVENTS_PER_SEC_OPTION_KEY)
    val retentionDurationMilliseconds = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_RETENTION_DURATION_MILLISECONDS_OPTION_KEY)
    val retentionSizeBytes = caseInsensitiveParams.get(PravegaSourceProvider.DEFAULT_RETENTION_SIZE_BYTES_OPTION_KEY)

    // set scaling policy
    streamConfig = (minSegments, scaleFactor, targetRateBytesPerSec, targetRateEventsPerSec) match {
      case (Some(minSegments), scaleFactor, targetRateKiloBytesPerSec, targetRateEventsPerSec) =>
        (scaleFactor, targetRateKiloBytesPerSec, targetRateEventsPerSec) match {
          case (Some(scaleFactor), Some(targetRateBytesPerSec), None) =>
            streamConfig.scalingPolicy(ScalingPolicy.byDataRate(targetRateBytesPerSec.toInt / 1024, scaleFactor.toInt, minSegments.toInt))
          case (Some(scaleFactor), None, Some(targetRateEventsPerSec)) =>
            streamConfig.scalingPolicy(ScalingPolicy.byEventRate(targetRateEventsPerSec.toInt, scaleFactor.toInt, minSegments.toInt))
          case _ =>
            streamConfig.scalingPolicy(ScalingPolicy.fixed(minSegments.toInt))
        }
      case _ => streamConfig
    }

    // set retention policy
    streamConfig = (retentionDurationMilliseconds, retentionSizeBytes) match {
      case (Some(retentionDurationMilliseconds), None) =>
        streamConfig.retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(retentionDurationMilliseconds.toLong)))
      case (None, Some(retentionSizeBytes)) =>
        streamConfig.retentionPolicy(RetentionPolicy.bySizeBytes(retentionSizeBytes.toLong))
      case _ =>
        streamConfig
    }

    log.info("streamConfig is {}", streamConfig)


    return streamConfig.build()
  }

  def getPravegaStreamCut(
                           params: Map[String, String],
                           streamCutOptionKey: String,
                           defaultStreamCut: PravegaStreamCut): PravegaStreamCut = {

    params.get(streamCutOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == STREAM_CUT_LATEST =>
        LatestStreamCut
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == STREAM_CUT_EARLIEST =>
        EarliestStreamCut
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == STREAM_CUT_UNBOUNDED =>
        UnboundedStreamCut
      case Some(base64String) => SpecificStreamCut(StreamCut.from(base64String))
      case None => defaultStreamCut
    }
  }
}

object PravegaReader {
  private[spark] val EVENT_FIELD_NAME = "event"
  private[spark] val SCOPE_FIELD_NAME = "scope"
  private[spark] val STREAM_FIELD_NAME = "stream"
  private[spark] val SEGMENT_ID_FIELD_NAME = "segment_id"
  private[spark] val OFFSET_FIELD_NAME = "offset"

  private[spark] val pravegaSchema: StructType = StructType(Seq(
    StructField(EVENT_FIELD_NAME, BinaryType),
    StructField(SCOPE_FIELD_NAME, StringType),
    StructField(STREAM_FIELD_NAME, StringType),
    StructField(SEGMENT_ID_FIELD_NAME, LongType),
    StructField(OFFSET_FIELD_NAME, LongType)
  ))
}
