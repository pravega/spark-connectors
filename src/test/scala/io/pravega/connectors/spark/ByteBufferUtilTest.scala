/**
  * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  */

package io.pravega.connectors.spark

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import org.junit.Assert._
import org.junit.Test

class ByteBufferUtilTest {

  @Test
  def concatenateTest(): Unit = {
    //Testing using Array of bytes as input
    val buffer1 = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    val buffer2 = ByteBuffer.wrap(Array[Byte](5, 6))
    val bytebuffer = ByteBufferUtil.concatenate(Seq(buffer1, buffer2))

    assertEquals(6, bytebuffer.capacity())
    assertEquals(3, bytebuffer.get(2))
    assertEquals(5, bytebuffer.get(4))
    assertEquals(6, bytebuffer.get(5))
    bytebuffer.put(0, 11)
    assertEquals(11, bytebuffer.get(0))
    assertEquals(6, bytebuffer.limit())
    assertEquals(6, bytebuffer.remaining())
    assertEquals(0, bytebuffer.position())
  }

  @Test
  def concatenateTest2(): Unit = {
    //Test with a string buffer as input
    val stringBuf1 = ByteBuffer.wrap("Message".getBytes(StandardCharsets.UTF_8))
    var stringBuffer = ByteBufferUtil.concatenate(Seq(stringBuf1))

    assertEquals(7, stringBuffer.limit())
    assertEquals(0, stringBuffer.position())
    assertEquals(7, stringBuffer.capacity())
    assertEquals(7, stringBuffer.remaining())
  }

  @Test
  def concatenateTest3(): Unit = {
    //Test with Seq of 2 string buffers as input
    val stringBuf2 = ByteBuffer.wrap("Welcomes".getBytes(StandardCharsets.UTF_8))
    val stringBuf3 = ByteBuffer.wrap("Pravega".getBytes(StandardCharsets.UTF_8))
    val stringBuffer = ByteBufferUtil.concatenate(Seq(stringBuf2, stringBuf3))

    assertEquals(15, stringBuffer.remaining())
    assertEquals(15, stringBuffer.capacity())
    assertEquals(0, stringBuffer.position())
    assertEquals(15, stringBuffer.limit())
    assertEquals(87, stringBuffer.get(0))
    assertEquals(101, stringBuffer.get(1))
    stringBuffer.position(3)
    assertEquals(99, stringBuffer.get())
    stringBuffer.position(8)
    assertEquals(80, stringBuffer.get())
  }

}

