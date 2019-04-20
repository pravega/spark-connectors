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

import io.pravega.client.stream.StreamCut
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Utilities for converting Pravega related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Read stream cut from json string
   */
  def streamCut(str: String): StreamCut = {
      StreamCut.from(Serialization.read[String](str))
  }

  /**
   * Write stream cut as json string
   */
  def streamCut(streamCut: StreamCut): String = {
    Serialization.write(streamCut.asText())
  }
}
