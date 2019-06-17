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
  def testArrayOfBytesAsBuffer(): Unit = {
    //Testing using Array of bytes as input
    val buffer1 = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    val buffer2 = ByteBuffer.wrap(Array[Byte](5, 6))
    val input = ByteBufferUtil.concatenate(Seq(buffer1, buffer2))

    assertEquals(3, input.get(2))
    assertEquals(5, input.get(4))
    assertEquals(6, input.get(5))
    assertEquals(6, input.limit())
    assertEquals(0, input.position())
  }

  @Test
  def testStringBufferAsBuffer(): Unit = {
    //Test with a string buffer as input
    val stringBuf1 = ByteBuffer.wrap("Message".getBytes(StandardCharsets.UTF_8))
    var stringInput = ByteBufferUtil.concatenate(Seq(stringBuf1))

    assertEquals(101, stringInput.get(1))
    assertEquals(7, stringInput.limit())
    assertEquals(0, stringInput.position())

  }

  @Test
  def testSequenceOfStringsAsBuffer(): Unit = {
    //Test with Seq of 2 string buffers as input
    val stringBuf2 = ByteBuffer.wrap("Welcomes".getBytes(StandardCharsets.UTF_8))
    val stringBuf3 = ByteBuffer.wrap("Pravega".getBytes(StandardCharsets.UTF_8))
    val stringSequenceInput = ByteBufferUtil.concatenate(Seq(stringBuf2, stringBuf3))

    assertEquals(15, stringSequenceInput.remaining())
    assertEquals(0, stringSequenceInput.position())
    assertEquals(15, stringSequenceInput.limit())
    assertEquals(87, stringSequenceInput.get(0))
    assertEquals(101, stringSequenceInput.get(1))
    stringSequenceInput.position(3)
    assertEquals(99, stringSequenceInput.get())
    stringSequenceInput.position(8)
    assertEquals(80, stringSequenceInput.get())
  }

}
