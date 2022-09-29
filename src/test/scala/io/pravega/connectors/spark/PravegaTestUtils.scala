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

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.{Executors, ScheduledExecutorService}
import java.{util => ju}

import com.google.common.base.Preconditions
import io.pravega.client.admin.{StreamInfo, StreamManager}
import io.pravega.client.stream._
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.test.integration.utils.SetupUtils
import org.apache.spark.internal.Logging
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import resource.managed
import org.scalatest.time.SpanSugar._

import scala.collection.JavaConverters._

/**
  * This is a helper class for Pravega test suites. This has the functionality to set up
  * and tear down local Pravega servers, and to write data using Pravega writers.
  */
class PravegaTestUtils extends Logging {
  private val SETUP_UTILS = new SetupUtils

  private val streamingTimeout: Span = 5.seconds

  // Kafka producer
  private var writer: EventStreamWriter[String] = _

  def setup(): Unit = {
    SETUP_UTILS.startAllServices()
  }

  def teardown(): Unit = {
    SETUP_UTILS.stopAllServices()
  }

  def controllerUri: String = {
    SETUP_UTILS.getControllerUri.toString
  }

  def scope: String = {
    SETUP_UTILS.getScope
  }

  def createTestStream(streamName: String, numSegments: Int = 1): Unit = {
    SETUP_UTILS.createTestStream(streamName, numSegments)
  }

  def getStringWriter(streamName: String): EventStreamWriter[String] = {
    Preconditions.checkNotNull(streamName)
    val clientFactory = SETUP_UTILS.getClientFactory
    clientFactory.createEventWriter(streamName, new UTF8StringSerializer, EventWriterConfig.builder.build)
  }

  /** Send the array of messages to the Kafka broker */
  def sendMessages(streamName: String, messages: Array[String]): Unit = {
    sendMessages(streamName, messages, None)
  }

  /** Send the array of messages to Pravega using specified routing key */
  def sendMessages(
                    streamName: String,
                    messages: Array[String],
                    routingKey: Option[Int]): Unit = {
    writer = getStringWriter(streamName)
    try {
      messages.foreach {
        m =>
          log.info(s"sendMessages: writing event '${m}' with routing key '${routingKey}'")
          routingKey match {
            case Some(r) => writer.writeEvent(r.toString, m).get()
            case None => writer.writeEvent(m).get()
          }
      }
    } finally {
      if (writer != null) {
        writer.close()
        writer = null
      }
    }
  }

  def getStreamInfo(streamName: String): StreamInfo = {
    val streamManager = StreamManager.create(SETUP_UTILS.getControllerUri)
    try {
      PravegaUtils.getStreamInfo(streamManager, scope, streamName)
    } finally {
      streamManager.close()
    }
  }

  def getLatestStreamCut(streamNames: Set[String]): StreamCut = {
    Preconditions.checkArgument(streamNames.size == 1)
    getStreamInfo(streamNames.head).getTailStreamCut
  }

  /**
    * Force stream to scale immediately and wait for it.
    *
    * @param numSegments  After scaling, there will be this many segments.
    */
  def setStreamSegments(streamName: String, numSegments: Int): Unit = this.synchronized {
    log.info(s"setStreamSegments: BEGIN: numSegments=$numSegments")
    for (streamManager <- managed(StreamManager.create(SETUP_UTILS.getControllerUri))) {
      // set min num of segments to numSegments
      streamManager.updateStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(numSegments)).build())

      val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor
      try {
        // Get current list of segments.
        val currentSegments = SETUP_UTILS.getController.getCurrentSegments(scope, streamName).get
        log.info(s"setStreamSegments: before scaling: currentSegments=$currentSegments")

        val sealedSegments: ju.List[java.lang.Long] = currentSegments.getSegments.asScala.map(_.getSegmentId).map(Long.box).toList.asJava

        // Calculate uniform distribution of key ranges.
        val newKeyRanges: ju.Map[java.lang.Double, java.lang.Double] =
          (0 until numSegments).map(i => Double.box(i.toDouble / numSegments) -> Double.box((i.toDouble + 1) / numSegments)).toMap.asJava
        val stream: Stream = Stream.of(scope, streamName)
        // Scale stream and wait.
        SETUP_UTILS.getController.scaleStream(stream, sealedSegments, newKeyRanges, executor).getFuture.get

        eventually(Timeout(streamingTimeout)) {
          // Get new list of segments.
          val newSegments = SETUP_UTILS.getController.getCurrentSegments(scope, streamName).get
          log.info(s"setStreamSegments: after scaling: newSegments=$newSegments")
          assert(newSegments.getSegments.size == numSegments)
        }
        log.info("setStreamSegments: END")
      } finally {
        executor.shutdown()
      }
    }
  }
}

object PravegaTestUtils {
  /**
    * Return a nice string representation of the exception. It will call "printStackTrace" to
    * recursively generate the stack trace including the exception and its causes.
    * From org.apache.spark.util.Utils.
    */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }
}
