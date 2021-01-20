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

import org.apache.spark.internal.Logging
import com.google.common.base.Preconditions
import io.pravega.client.ClientConfig
import io.pravega.client.EventStreamClientFactory
import io.pravega.client.admin.StreamManager
import io.pravega.client.control.impl.{Controller, ControllerImpl, ControllerImplConfig}
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.stream.StreamConfiguration
import io.pravega.common.concurrent.ExecutorServiceHelpers
import javax.annotation.concurrent.NotThreadSafe
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ScheduledExecutorService
import org.apache.commons.lang3.RandomStringUtils

/**
  * Utility functions for creating the test setup.
  */

@NotThreadSafe
class SetupUtils extends Logging {
  private var controller: Controller = _
  private var executor: ScheduledExecutorService = _
  private var clientFactory: EventStreamClientFactory = _

  private val controllerUri = URI.create(System.getProperty("pravega.uri", "tcp://localhost:9090"))

  // Manage the state of the class
  private val started = new AtomicBoolean(false)

  def getClientConfig: ClientConfig = ClientConfig.builder.controllerURI(controllerUri).build

  // The test Scope name.
  private final val scope = RandomStringUtils.randomAlphabetic(20)

  /**
    * Start all pravega related services required for the test deployment.
    *
    * @throws Exception on any errors.
    */
  @throws[Exception]
  def startAllServices(): Unit = {
    if (!this.started.compareAndSet(false, true)) {
      log.warn("Services already started, not attempting to start again")
      return
    }

    executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "Controller pool")
    controller = new ControllerImpl(ControllerImplConfig.builder.clientConfig(getClientConfig).build, executor)
    clientFactory = EventStreamClientFactory.withScope(this.scope, getClientConfig)
  }

  /**
    * Stop the pravega cluster and release all resources.
    *
    * @throws Exception on any errors.
    */
  @throws[Exception]
  def stopAllServices(): Unit = {
    if (!started.compareAndSet(true, false)) {
      log.warn("Services not yet started or already stopped, not attempting to stop")
      return
    }
    if (clientFactory != null) {
      clientFactory.close
    }
    if (controller != null) {
      controller.close
    }
    if (executor != null) {
      executor.shutdown
    }
  }

  /**
    * Fetch the controller endpoint for the cluster.
    *
    * @return URI The controller endpoint to connect to this cluster.
    */
  def getControllerUri: URI = controllerUri

  /**
    * Fetch the test Scope name.
    *
    * @return String The test Scope name.
    */
  def getScope: String = scope

  /**
    * Fetch the Controller for the cluster.
    *
    * @return Controller The instance of the Controller client.
    */
  def getController: Controller = controller

  /**
    * Fetch the Event Stream Client Factory for the cluster.
    *
    * @return EventStreamClientFactory The instance of the Client Factory.
    */
  def getClientFactory: EventStreamClientFactory = clientFactory

  /**
    * Create the test stream.
    *
    * @param streamName  Name of the test stream.
    * @param numSegments Number of segments to be created for this stream.
    * @throws Exception on any errors.
    */
  @throws[Exception]
  def createTestStream(streamName: String, numSegments: Int): Unit = {
    Preconditions.checkState(started.get, "%s", "Services not yet started")
    Preconditions.checkNotNull(streamName)
    Preconditions.checkArgument(numSegments > 0)
    val streamManager = StreamManager.create(getClientConfig)
    streamManager.createScope(scope)
    streamManager.createStream(scope, streamName, StreamConfiguration.builder.scalingPolicy(ScalingPolicy.fixed(numSegments)).build)
    log.info("Created stream: " + streamName)
    streamManager.close
  }
}
