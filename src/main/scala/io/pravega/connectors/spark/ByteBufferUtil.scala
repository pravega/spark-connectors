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

object ByteBufferUtil {
  def concatenate(buffers: Seq[ByteBuffer]): ByteBuffer = {
    val totalSize = buffers.map(_.remaining).sum
    val output = ByteBuffer.allocate(totalSize)
    buffers.foreach(output.put)
    output.flip()
    output
  }
}
