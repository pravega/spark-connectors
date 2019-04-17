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

import java.nio.ByteBuffer
import java.util.UUID

import io.pravega.client.stream.impl.ByteBufferSerializer
import io.pravega.client.stream.{EventStreamWriter, EventWriterConfig, Transaction, TxnFailedException}
import io.pravega.client.{ClientConfig, ClientFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

case class PravegaWriterCommitMessage(transactionId: UUID) extends WriterCommitMessage

/**
  * A [[StreamWriter]] for Pravega writing. Responsible for generating the writer factory.
  * It uses Pravega transactions to support exactly-once semantics.
  *
  * @param transactionTimeoutTime   The number of milliseconds for transactions to timeout.
  * @param schema                   The schema of the input data.
  */
class PravegaStreamWriter(
                           scopeName: String,
                           streamName: String,
                           clientConfig: ClientConfig,
                           transactionTimeoutTime: Long,
                           schema: StructType)
  extends StreamWriter with Logging {

  private lazy val clientFactory = ClientFactory.withScope(scopeName, clientConfig)
  private lazy val writer: EventStreamWriter[ByteBuffer] = clientFactory.createEventWriter(
    streamName,
    new ByteBufferSerializer,
    EventWriterConfig.builder
      .transactionTimeoutTime(transactionTimeoutTime)
      .build)

  override def createWriterFactory(): PravegaStreamWriterFactory =
    PravegaStreamWriterFactory(scopeName, streamName, clientConfig, transactionTimeoutTime, schema)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    messages
      .map(_.asInstanceOf[PravegaWriterCommitMessage])
      .map(_.transactionId)
      .filter(_ != null)
      .par
      .foreach { transactionId =>
      val transaction = writer.getTxn(transactionId)
      val status = transaction.checkStatus
      if (status == Transaction.Status.COMMITTED || status == Transaction.Status.COMMITTING) {
        log.info(s"commit: transaction=${transactionId}, already committed")
      } else if (status == Transaction.Status.OPEN) {
        log.info(s"commit: transaction=${transactionId}, committing")
        transaction.commit()
      } else {
        // ABORTED or ABORTING
        throw new TxnFailedException()
      }
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    messages
      .map(_.asInstanceOf[PravegaWriterCommitMessage])
      .map(_.transactionId)
      .filter(_ != null)
      .par
      .foreach { transactionId =>
        val transaction = writer.getTxn(transactionId)
        val status = transaction.checkStatus
        if (status == Transaction.Status.OPEN) {
          log.info(s"abort: transaction=${transactionId}, aborting")
          transaction.abort()
        }
      }
  }
}

/**
  * A [[DataWriterFactory]] for Pravega writing. It will be serialized and sent to executors to
  * generate the per-task data writers.
  *
  * @param transactionTimeoutTime   The number of milliseconds for transactions to timeout.
  * @param schema                   The schema of the input data.
  */
case class PravegaStreamWriterFactory(
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
    new PravegaStreamDataWriter(scopeName, streamName, clientConfig, transactionTimeoutTime, schema)
  }
}

/**
  * A [[DataWriter]] for Pravega writing. One data writer will be created in each partition to
  * process incoming rows.
  *
  * @param transactionTimeoutTime   The number of milliseconds for transactions to timeout.
  * @param inputSchema              The attributes in the input data.
  */
class PravegaStreamDataWriter(
                               scopeName: String,
                               streamName: String,
                               clientConfig: ClientConfig,
                               transactionTimeoutTime: Long,
                               inputSchema: StructType)
  extends DataWriter[InternalRow] with Logging {

  val ROUTING_KEY_ATTRIBUTE_NAME: String = "routing_key"
  val EVENT_ATTRIBUTE_NAME: String = "event"

  protected val projection = createProjection

  private lazy val clientFactory = ClientFactory.withScope(scopeName, clientConfig)
  private lazy val writer: EventStreamWriter[ByteBuffer] = clientFactory.createEventWriter(
    streamName,
    new ByteBufferSerializer,
    EventWriterConfig
      .builder
      .transactionTimeoutTime(transactionTimeoutTime)
      .build)

  private var transaction: Transaction[ByteBuffer] = null

  def write(row: InternalRow): Unit = {
    log.info(s"write: row=${row}")
    val projectedRow = projection(row)
    val event = projectedRow.getBinary(1)
    if (transaction == null) {
      transaction = writer.beginTxn()
      log.info(s"write: began transaction ${transaction.getTxnId}")
    }

    val haveRoutingKey = !projectedRow.isNullAt(0)
    if (haveRoutingKey) {
      val routingKey = projectedRow.getUTF8String(0).toString
      log.info(s"write: routingKey=${routingKey}, event=${event}")
      transaction.writeEvent(routingKey, ByteBuffer.wrap(event))
    } else {
      transaction.writeEvent(ByteBuffer.wrap(event))
      log.info(s"write: event=${event}")
    }
  }

  def commit(): WriterCommitMessage = {
    if (transaction == null) {
      PravegaWriterCommitMessage(null)
    } else {
      log.info(s"commit: transaction ${transaction.getTxnId}")
      transaction.flush()
      val transactionId = transaction.getTxnId
      transaction = null
      PravegaWriterCommitMessage(transactionId)
    }
  }

  def abort(): Unit = {
    if (transaction != null) {
      transaction.abort()
      transaction = null
    }
  }

  def close(): Unit = {
    if (transaction != null) {
      transaction.abort()
      transaction = null
    }
    if (writer != null) {
      writer.close()
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
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
          s"attribute unsupported type ${t.catalogString}")
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
          s"attribute unsupported type ${t.catalogString}")
    }

    UnsafeProjection.create(
      Seq(
        Cast(keyExpression, StringType),
        Cast(eventExpression, BinaryType)),
      attributes)
  }
}
