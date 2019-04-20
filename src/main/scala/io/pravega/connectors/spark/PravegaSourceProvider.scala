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

import java.net.URI
import java.util.{Locale, Optional}

import io.pravega.client.ClientConfig
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.StreamConfiguration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._


object Encoding extends Enumeration {
  type Encoding = Value
  val None: Value = Value("none")
  val Chunked_v1: Value = Value("chunked_v1")
}

class PravegaSourceProvider extends DataSourceV2
  with MicroBatchReadSupport
  with DataSourceRegister
  with StreamWriteSupport
  with Logging {

  private val CONTROLLER = "controller"
  private val SCOPE = "scope"
  private val STREAM = "stream"
  private val TRANSACTION_TIMEOUT_MS = "transaction_timeout_ms"
  private val ENCODING_KEY = "encoding"

  private val DEFAULT_CONTROLLER = "tcp://localhost:9090"
  private val DEFAULT_TRANSACTION_TIMEOUT_MS: Long = 30000

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "pravega"

  override def createMicroBatchReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): MicroBatchReader = {

    val parameters = options.asMap().asScala.toMap
    validateStreamOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    val controllerURI = URI.create(caseInsensitiveParams.getOrElse(CONTROLLER, "tcp://localhost:9090"))
    val scopeName = caseInsensitiveParams.getOrElse(SCOPE, "")
    val streamName = caseInsensitiveParams.getOrElse(STREAM, "")
    val encoding = Encoding.withName(caseInsensitiveParams.getOrElse(ENCODING_KEY, Encoding.None.toString))

    log.info(s"createMicroBatchReader: controllerURI=${controllerURI}, scopeName=${scopeName}, streamName=${streamName}, encoding=${encoding}")

    val clientConfig = ClientConfig.builder()
      .controllerURI(controllerURI)
      .build()
    createStream(scopeName, streamName, clientConfig)
    new PravegaMicroBatchReader(
      scopeName,
      streamName,
      clientConfig,
      encoding,
      options)
  }

  /**
    * Creates an optional {@link StreamWriter} to save the data to this data source. Data
    * sources can return None if there is no writing needed to be done.
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
    validateStreamOptions(parameters)
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    val controllerURI = URI.create(caseInsensitiveParams.getOrElse(CONTROLLER, DEFAULT_CONTROLLER))
    val scopeName = caseInsensitiveParams.getOrElse(SCOPE, "")
    val streamName = caseInsensitiveParams.getOrElse(STREAM, "")
    val transactionTimeoutTime = caseInsensitiveParams.get(TRANSACTION_TIMEOUT_MS) match {
      case Some(s: String) => s.toLong
      case None => DEFAULT_TRANSACTION_TIMEOUT_MS
    }

    log.info(s"createStreamWriter: controllerURI=${controllerURI}, scopeName=${scopeName}, streamName=${streamName}, transactionTimeoutTime=${transactionTimeoutTime}")

    val clientConfig = ClientConfig.builder()
      .controllerURI(controllerURI)
      .build()
    createStream(scopeName, streamName, clientConfig)

    new PravegaStreamWriter(scopeName, streamName, clientConfig, transactionTimeoutTime, schema)
  }

  private def validateStreamOptions(caseInsensitiveParams: Map[String, String]): Unit = {
    // TODO
  }

  private def createStream(scopeName: String, streamName: String, clientConfig: ClientConfig): Unit = {
    val streamManager = StreamManager.create(clientConfig)
    try {
      streamManager.createScope(scopeName)
      streamManager.createStream(scopeName, streamName,
        StreamConfiguration.builder
          .scope(scopeName)
          .streamName(streamName)
          // TODO: set scaling policy
          .build())
    } finally {
      streamManager.close()
    }
  }
}
