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

import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{TransactionalEventStreamWriter, EventWriterConfig, Transaction, TxnFailedException}
import io.pravega.client.{ClientConfig, EventStreamClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}
import io.pravega.connectors.spark.PravegaSourceProvider._
import resource.managed

case class PravegaWriterCommitMessage(transactionId: UUID) extends WriterCommitMessage

/**
  * Both a [[StreamWriter]] and a [[DataSourceWriter]] for Pravega writing.
  * Responsible for generating the writer factory.
  * It uses Pravega transactions to support exactly-once semantics.
  * Used by both streaming and batch jobs.
  *
  * @param transactionTimeoutMs            The number of milliseconds for transactions to timeout.
  * @param readAfterWriteConsistency       If true, commit() does not return until the events are
  *                                        visible to readers. If false, commit() returns as soon as
  *                                        transactions enter the committing phase and events may
  *                                        not be immediately visible to readers.
  * @param transactionStatusPollIntervalMs If readAfterWriteConsistency is true, the transaction will be polled
  *                                        with this interval of milliseconds.
  * @param schema                          The schema of the input data.
  *
  */
class PravegaWriter(
                     scopeName: String,
                     streamName: String,
                     clientConfig: ClientConfig,
                     transactionTimeoutMs: Long,
                     readAfterWriteConsistency: Boolean,
                     transactionStatusPollIntervalMs: Long,
                     schema: StructType)
  extends DataSourceWriter with StreamWriter with Logging {

  private def createClientFactory: EventStreamClientFactory = {
    EventStreamClientFactory.withScope(scopeName, clientConfig)
  }

  private def createWriter(clientFactory: EventStreamClientFactory): TransactionalEventStreamWriter[ByteBuffer] = {
    clientFactory.createTransactionalEventWriter(
      streamName,
      new ByteBufferSerializer,
      EventWriterConfig.builder
        .transactionTimeoutTime(transactionTimeoutMs)
        .build)
  }

  override def createWriterFactory(): PravegaWriterFactory =
    PravegaWriterFactory(scopeName, streamName, clientConfig, transactionTimeoutMs, schema)

  /**
    * To allow allows read-after-write consistency, we want to wait until the transaction transitions to COMMITTED
    * which indicates that readers can view the events.
    */
  private def waitForCommittedTransaction(transaction: Transaction[ByteBuffer]): Unit = {
    if (readAfterWriteConsistency) {
      var status: Transaction.Status = transaction.checkStatus
      while (status == Transaction.Status.COMMITTING) {
        Thread.sleep(transactionStatusPollIntervalMs)
        status = transaction.checkStatus
      }
      if (status != Transaction.Status.COMMITTED) {
        // This should never happen.
        log.error(s"waitForCommittedTransaction: Transaction ${transaction.getTxnId} changed from COMMITTING to ${status}")
        throw new TxnFailedException()
      }
      log.info(s"waitForCommittedTransaction: transaction=${transaction.getTxnId}, committed")
    }
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    log.debug(s"commit: BEGIN: epochId=$epochId, messages=${messages.mkString(",")}")
    for {
      clientFactory <- managed(createClientFactory)
      writer <- managed(createWriter(clientFactory))
    } {
      messages
        .map(_.asInstanceOf[PravegaWriterCommitMessage])
        .map(_.transactionId)
        .filter(_ != null)
        .par
        .foreach { transactionId =>
          val transaction = writer.getTxn(transactionId)
          log.info(s"commit: transaction=${transactionId}, calling commit")
          transaction.commit()
          log.info(s"commit: transaction=${transactionId}, committing")
          waitForCommittedTransaction(transaction)
        }
    }
    log.debug(s"commit: END: epochId=$epochId, messages=${messages.mkString(",")}")
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    log.debug(s"abort: BEGIN: epochId=$epochId, messages=${messages.mkString(",")}")
    for {
      clientFactory <- managed(createClientFactory)
      writer <- managed(createWriter(clientFactory))
      // TODO: An exception is thrown by the Pravega 0.4.0 client here for an unknown reason. The transaction will still abort when it times out.
    } {
      messages
        .map(_.asInstanceOf[PravegaWriterCommitMessage])
        .map(_.transactionId)
        .filter(_ != null)
        .par
        .foreach { transactionId =>
          log.info(s"abort: transaction=${transactionId}, aborting")
          val transaction = writer.getTxn(transactionId)
          try {
            transaction.abort()
          } catch {
            case ex: Throwable =>
              // Log but ignore any errors.
              log.info("abort: Ignoring exception during abort()", ex)
          }
        }
    }
    log.debug(s"abort: END: epochId=$epochId, messages=${messages.mkString(",")}")
  }

  // Used for batch writer.
  override def commit(messages: Array[WriterCommitMessage]): Unit = commit(0, messages)

  // Used for batch writer.
  override def abort(messages: Array[WriterCommitMessage]): Unit = abort(0, messages)
}

/**
  * A [[DataWriterFactory]] for Pravega writing. It will be serialized and sent to executors to
  * generate the per-task data writers.
  *
  * @param transactionTimeoutTime   The number of milliseconds for transactions to timeout.
  * @param schema                   The schema of the input data.
  */
case class PravegaWriterFactory(
                                       scopeName: String,
                                       streamName: String,
                                       clientConfig: ClientConfig,
                                       transactionTimeoutTime: Long,
                                       schema: StructType)
  extends DataWriterFactory[InternalRow] with Logging {

  override def createDataWriter(
                                 partitionId: Int,
                                 taskId: Long,
                                 epochId: Long): DataWriter[InternalRow] = {
    new PravegaDataWriter(scopeName, streamName, clientConfig, transactionTimeoutTime, schema)
  }
}

/**
  * A [[DataWriter]] for Pravega writing. One data writer will be created in each partition to
  * process incoming rows.
  *
  * @param transactionTimeoutTime   The number of milliseconds for transactions to timeout.
  * @param inputSchema              The attributes in the input data.
  */
class PravegaDataWriter(
                               scopeName: String,
                               streamName: String,
                               clientConfig: ClientConfig,
                               transactionTimeoutTime: Long,
                               inputSchema: StructType)
  extends DataWriter[InternalRow] with Logging {

  private val projection = createProjection

  private val clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig)
  private val writer: TransactionalEventStreamWriter[ByteBuffer] = clientFactory.createTransactionalEventWriter(
    streamName,
    new ByteBufferSerializer,
    EventWriterConfig
      .builder
      .transactionTimeoutTime(transactionTimeoutTime)
      .build)

  private var transaction: Transaction[ByteBuffer] = _

  override def write(row: InternalRow): Unit = {
    val projectedRow = projection(row)
    val event = projectedRow.getBinary(1)
    val eventToLog = if (log.isDebugEnabled) {
      val s = new String(event, StandardCharsets.UTF_8)
      val maxLength = 100
      if (s.length > maxLength) s.substring(0, maxLength) + "..." else s
    } else null

    if (transaction == null) {
      transaction = writer.beginTxn()
      log.info(s"write: transaction=${transaction.getTxnId}, begin")
    }

    val haveRoutingKey = !projectedRow.isNullAt(0)
    if (haveRoutingKey) {
      val routingKey = projectedRow.getUTF8String(0).toString
      log.debug(s"write: routingKey=${routingKey}, event=${eventToLog}")
      transaction.writeEvent(routingKey, ByteBuffer.wrap(event))
    } else {
      transaction.writeEvent(ByteBuffer.wrap(event))
      log.debug(s"write: event=${eventToLog}")
    }
  }

  override def commit(): WriterCommitMessage = {
    try {
      if (transaction == null) {
        PravegaWriterCommitMessage(null)
      } else {
        log.info(s"commit: transaction=${transaction.getTxnId}, flushing")
        transaction.flush()
        log.debug(s"commit: transaction=${transaction.getTxnId}, flushed, sending transaction ID to driver for commit")
        val transactionId = transaction.getTxnId
        transaction = null
        PravegaWriterCommitMessage(transactionId)
      }
    } finally {
      close()
    }
  }

  override def abort(): Unit = {
    try {
      if (transaction != null) {
        log.info(s"abort: transaction=${transaction.getTxnId}, aborting")
        transaction.abort()
        log.debug(s"abort: transaction=${transaction.getTxnId}, aborted")
        transaction = null
      }
    } finally {
      close()
    }
  }

  private def close(): Unit = {
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
