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

import io.pravega.client.stream.StreamCut

/**
 * Objects that represent desired StreamCut for starting,
 * ending, and specific StreamCuts.
 */
sealed trait PravegaStreamCut

/**
 * Represents the desire to bind to the earliest StreamCut in Pravega.
 * The earliest StreamCut is evaluated when the reader is initialized.
 */
case object EarliestStreamCut extends PravegaStreamCut

/**
 * Represents the desire to bind to the latest StreamCut in Pravega.
 * The latest StreamCut is evaluated when the reader is initialized.
 */
case object LatestStreamCut extends PravegaStreamCut

/**
 * Represents the desire to bind to the unbounded StreamCut in Pravega.
 * A streaming reader with an unbounded StreamCut will attempt to read forever.
 * A batch reader will treat this as LatestStreamCut.
 */
case object UnboundedStreamCut extends PravegaStreamCut

/**
 * Represents the desire to bind to a specific StreamCut.
 */
case class SpecificStreamCut(streamCuts: StreamCut) extends PravegaStreamCut
