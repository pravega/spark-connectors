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
import java.util.Locale

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.{RetentionPolicy, ScalingPolicy, StreamConfiguration, StreamCut}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import resource.managed

import scala.collection.JavaConverters._
import scala.util.Try

object MetadataTableName extends Enumeration {
  type MetadataTableName = Value
  val StreamInfo: Value = Value("StreamInfo")
  val Streams: Value = Value("Streams")
}

class PravegaSourceProvider extends DataSourceRegister
  with SimpleTableProvider
  with Logging {


  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = PravegaSourceProvider.SOURCE_PROVIDER_NAME


  /**
   * Return a {@link Table} instance with the specified table schema, partitioning and properties
   * to do read/write. The returned table should report the same schema and partitioning with the
   * specified ones, or Spark may fail the operation.
   */
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val caseInsensitiveParams = CaseInsensitiveMap(options.asScala.toMap)
    PravegaSourceProvider.validateOptions(caseInsensitiveParams)
    val metadataTableName = caseInsensitiveParams.get(PravegaSourceProvider.METADATA_OPTION_KEY).map(MetadataTableName.withName)
    if (metadataTableName.isDefined) {
      log.debug("return PravegaMetaTable")
      new PravegaMetaTable(metadataTableName.get)
    } else {
      log.debug("return PravegaTable")
      new PravegaTable()
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

  private[spark] val DEFAULT_CONTROLLER = "tcp://localhost:9090"
  private[spark] val DEFAULT_TRANSACTION_TIMEOUT_MS: Long = 30 * 1000
  private[spark] val DEFAULT_BATCH_TRANSACTION_TIMEOUT_MS: Long = 2 * 60 * 1000 // 2 minutes (maximum allowed by default server)
  private[spark] val DEFAULT_TRANSACTION_STATUS_POLL_INTERVAL_MS: Long = 50

  private[spark] val MAX_OFFSET_PER_TRIGGER = "maxoffsetspertrigger"

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

  def validateOptions(caseInsensitiveParams: Map[String, String]): Unit = {

    // ValidateGeneralOptions
    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.SCOPE_OPTION_KEY, "").isEmpty) {
      throw new IllegalArgumentException(s"Missing required option '${PravegaSourceProvider.SCOPE_OPTION_KEY}'")
    }
    if (caseInsensitiveParams.getOrElse(PravegaSourceProvider.STREAM_OPTION_KEY, "").isEmpty) {
      throw new IllegalArgumentException(s"Missing required option '${PravegaSourceProvider.STREAM_OPTION_KEY}'")
    }

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

    val maxOffsetPerTrigger = caseInsensitiveParams.get(PravegaSourceProvider.MAX_OFFSET_PER_TRIGGER)
    if (maxOffsetPerTrigger.isDefined && (Try(maxOffsetPerTrigger.get.toInt).isFailure || maxOffsetPerTrigger.get.toInt < 1))
    {
      throw new IllegalArgumentException(s"Max events per trigger should be an integer more than or equal to one")
    }
  }

  def buildClientConfig(caseInsensitiveParams: Map[String, String]): ClientConfig = {
    val controllerURI = URI.create(caseInsensitiveParams.getOrElse(PravegaSourceProvider.CONTROLLER_OPTION_KEY, DEFAULT_CONTROLLER))
    ClientConfig.builder()
      .controllerURI(controllerURI)
      .build()
  }

  def createStream(caseInsensitiveParams: Map[String, String]): Unit = {
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
