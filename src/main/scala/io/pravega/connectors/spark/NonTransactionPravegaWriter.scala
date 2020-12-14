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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{EventStreamWriter, EventWriterConfig}
import io.pravega.client.{ClientConfig, EventStreamClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfo}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import io.pravega.connectors.spark.PravegaSourceProvider._

import scala.compat.java8.FutureConverters._

import org.apache.spark.sql.connector.write.WriterCommitMessage

import scala.concurrent.ExecutionContext.Implicits.global

case class NonTransactionPravegaWriterCommitMessage(transactionId: UUID) extends WriterCommitMessage

/**
  * Both a [[StreamingWrite]] and a [[BatchWrite]] for Pravega writing.
  * Responsible for generating the writer factory.
  * It uses Pravega transactions to support exactly-once semantics.
  * Used by both streaming and batch jobs.
  *
//  * @param transactionTimeoutMs            The number of milliseconds for transactions to timeout.
  * @param schema                          The schema of the input data.
  *
  */
class NonTransactionPravegaWriter(
                     scopeName: String,
                     streamName: String,
                     clientConfig: ClientConfig,
                     schema: StructType)
  extends StreamingWrite with BatchWrite with Logging {

  private def createClientFactory: EventStreamClientFactory = {
    EventStreamClientFactory.withScope(scopeName, clientConfig)
  }

  private def createWriter(clientFactory: EventStreamClientFactory): EventStreamWriter[ByteBuffer] = {
    clientFactory.createEventWriter(
      streamName,
      new ByteBufferSerializer,
      EventWriterConfig.builder
        .build)
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo):  NonTransactionPravegaWriterFactory =
    NonTransactionPravegaWriterFactory(scopeName, streamName, clientConfig, schema)

  // Used for batch writer.
  override def commit(messages: Array[WriterCommitMessage]):  Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): NonTransactionPravegaWriterFactory =
    NonTransactionPravegaWriterFactory(scopeName, streamName, clientConfig, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

/**
  * A [[DataWriterFactory]] for Pravega writing. It will be serialized and sent to executors to
  * generate the per-task data writers.
  *
  * @param schema                   The schema of the input data.
  */
case class NonTransactionPravegaWriterFactory(
                                       scopeName: String,
                                       streamName: String,
                                       clientConfig: ClientConfig,
                                       schema: StructType)
  extends DataWriterFactory with StreamingDataWriterFactory with Logging {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new NonTransactionPravegaDataWriter(scopeName, streamName, clientConfig, schema)
  }

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new NonTransactionPravegaDataWriter(scopeName, streamName, clientConfig, schema)
  }
}

/**
  * A [[DataWriter]] for Pravega writing. One data writer will be created in each partition to
  * process incoming rows.
  *
  * @param inputSchema              The attributes in the input data.
  */
class NonTransactionPravegaDataWriter(
                               scopeName: String,
                               streamName: String,
                               clientConfig: ClientConfig,
                               inputSchema: StructType)
  extends DataWriter[InternalRow] with Logging {

  val pendingWriteCounts = new AtomicInteger(0)

  private val projection = createProjection

  private val clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig)
  private val writer: EventStreamWriter[ByteBuffer] = clientFactory.createEventWriter(
    streamName,
    new ByteBufferSerializer,
    EventWriterConfig
      .builder
      .build)


  override def write(row: InternalRow): Unit = {
    val projectedRow = projection(row)
    val event = projectedRow.getBinary(1)
    val eventToLog = if (log.isDebugEnabled) {
      val s = new String(event, StandardCharsets.UTF_8)
      val maxLength = 100
      if (s.length > maxLength) s.substring(0, maxLength) + "..." else s
    } else null

    log.info(s"write: begin")

    val haveRoutingKey = !projectedRow.isNullAt(0)
    if (haveRoutingKey) {
      val routingKey = projectedRow.getUTF8String(0).toString
      log.debug(s"write: routingKey=${routingKey}, event=${eventToLog}")
      writer.writeEvent(routingKey, ByteBuffer.wrap(event))
    } else {
      log.debug(s"write: event=${eventToLog}")

      val f = toScala(writer.writeEvent(ByteBuffer.wrap(event)))
      f
        .map(println)
        .recover { case e =>
          e.printStackTrace
          "recovered"
        }.map(println)
    }
    log.debug(s"write: end")
  }

  override def commit(): WriterCommitMessage = {
    log.debug(s"commit: begin")
    try {
      writer.flush()
      NonTransactionPravegaWriterCommitMessage(null)
    } finally {
      close()
      log.debug(s"commit: end")
    }
  }

  override def abort(): Unit = {
    log.debug("abort:BEGIN")
      close()
    log.debug("abort:END")
  }

  override def close(): Unit = {
    log.debug("close: BEGIN")
    try {
      writer.close()
    } catch {
      case e: Throwable => log.warn("close: exception during writer close", e)
    }
    try {
      clientFactory.close()
    } catch {
      case e: Throwable => log.warn("close: exception during client factory close", e)
    }
    log.debug("close: END")
  }

  private def createProjection = {
    val attributes = inputSchema
      .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())

    val keyExpression = attributes
      .find(_.name == ROUTING_KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, StringType))
    keyExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(s"$ROUTING_KEY_ATTRIBUTE_NAME " +
          s"attribute type must be a string; received unsupported type ${t.catalogString}")
    }

    val eventExpression = attributes
      .find(_.name == EVENT_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'$EVENT_ATTRIBUTE_NAME' not found")
    )
    eventExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"$EVENT_ATTRIBUTE_NAME " +
          s"attribute type must be a string or binary; received unsupported type ${t.catalogString}")
    }

    UnsafeProjection.create(
      Seq(
        Cast(keyExpression, StringType),
        Cast(eventExpression, BinaryType)),
      attributes)
  }
}
