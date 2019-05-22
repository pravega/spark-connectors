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

import java.io.{PrintWriter, StringWriter}

import com.google.common.base.Preconditions
import io.pravega.client.admin.{StreamInfo, StreamManager}
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream._
import io.pravega.test.integration.utils.SetupUtils
import org.apache.spark.internal.Logging
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.time.SpanSugar._

/**
 * This is a helper class for Pravega test suites. This has the functionality to set up
 * and tear down local Pravega servers, and to write data using Pravega writers.
 */
class PravegaTestUtils extends Logging {
  private val SETUP_UTILS = new SetupUtils

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
      streamManager.getStreamInfo(scope, streamName)
    } finally {
      streamManager.close()
    }
  }

  def getLatestStreamCut(streamNames: Set[String]): StreamCut = {
    Preconditions.checkArgument(streamNames.size == 1)
    getStreamInfo(streamNames.head).getTailStreamCut
  }

  def setStreamSegments(streamName: String, numSegments: Int): Unit = {
    val streamManager = StreamManager.create(SETUP_UTILS.getControllerUri)
    try {
      streamManager.updateStream(scope, streamName,
        StreamConfiguration.builder
          .scalingPolicy(ScalingPolicy.fixed(numSegments))
          .build)
      // TODO: Below does not work
      //      eventually(timeout(60.seconds)) {
      //        assert(getStreamInfo(streamName).getTailStreamCut.asImpl().getPositions.size == numSegments)
      //      }
    }
    finally {
      streamManager.close()
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
