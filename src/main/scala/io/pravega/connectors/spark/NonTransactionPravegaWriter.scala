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
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiConsumer

import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{EventStreamWriter, EventWriterConfig, Transaction, TransactionalEventStreamWriter, TxnFailedException}
import io.pravega.client.{ClientConfig, EventStreamClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import io.pravega.connectors.spark.PravegaSourceProvider._
import resource.managed

import scala.compat.java8.FutureConverters._
import java.util.function._

import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext.Implicits.global

case class NonTransactionPravegaWriterCommitMessage(transactionId: UUID) extends WriterCommitMessage

/**
  * Both a [[StreamWriter]] and a [[DataSourceWriter]] for Pravega writing.
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
//                     transactionTimeoutMs: Long,
//                     readAfterWriteConsistency: Boolean,
//                     executorService: ExecutorService,
//                     transactionStatusPollIntervalMs: Long,
                     schema: StructType)
  extends DataSourceWriter with StreamWriter with Logging {

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

  override def createWriterFactory(): NonTransactionPravegaWriterFactory =
    NonTransactionPravegaWriterFactory(scopeName, streamName, clientConfig, schema)

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  // Used for batch writer.
  override def commit(messages: Array[WriterCommitMessage]): Unit = commit(0, messages)

  // Used for batch writer.
//  override def abort(messages: Array[WriterCommitMessage]): Unit = abort(0, messages)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
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
  extends DataWriterFactory[InternalRow] with Logging {

  override def createDataWriter(
                                 partitionId: Int,
                                 taskId: Long,
                                 epochId: Long): DataWriter[InternalRow] = {
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

//  private var transaction: Transaction[ByteBuffer] = _

  override def write(row: InternalRow): Unit = {
//    pendingWriteCounts.incrementAndGet()

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

      val f = (writer.writeEvent(ByteBuffer.wrap(event)))
//      val f = toScala(writer.writeEvent(ByteBuffer.wrap(event)))
//      val f = writer.writeEvent(ByteBuffer.wrap(event)).exceptionally(e => {println(e); return null}).toScala
//      f.exceptionally().toScala
//      f
//        .map(println)
//        .recover { case e =>
//          e.printStackTrace
//          "recovered"
//        }.map(println)


//        handle((r, e) => {
//        if (e != null) {
//          if (e.isInstanceOf[IndexOutOfBoundsException]) throw new IllegalArgumentException
//          throw e.asInstanceOf[RuntimeException] // this is sketchy, handle it differently, maybe by wrapping it in a RuntimeException
//
//        }
//      })
//        .onComplete(_ => log.debug(s"Event as complete"))
//      val foo: Int => Boolean = i => i > 7
//
//
      f.whenComplete( new BiConsumer[Void, Throwable] {
        log.debug(s"Event as complete")

        override def accept(t: Void, u: Throwable): Unit = {
          if (t != null) {
            f.complete(t)
            log.debug(s"Event as complete")
          }
          if (u != null) {
            f.completeExceptionally(u)
            log.debug(u.getMessage)
          }
        }
      })
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
//    try {
//      transaction.abort()
//      if (transaction != null) {
//        log.info(s"abort: transaction=${transaction.getTxnId}, aborting")
//        transaction.abort()
//        log.debug(s"abort: transaction=${transaction.getTxnId}, aborted")
//        transaction = null
//      }
//    } finally {
    log.debug("abort:BEGIN")
      close()
    log.debug("abort:END")
    //    }
  }

  private def close(): Unit = {
    log.debug("close: BEGIN")
    try {
      writer.flush()
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
