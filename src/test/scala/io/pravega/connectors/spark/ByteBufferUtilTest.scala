package io.pravega.connectors.spark

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import org.junit.Assert.assertEquals
import org.junit.Test

class ByteBufferUtilTest {

  @Test
  def concatenateTest(): Unit = {

    val a = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    val b = ByteBuffer.wrap(Array[Byte](5, 6))
    val c = ByteBufferUtil.concatenate(Seq(a, b))

    assertEquals(6, c.capacity())
    assertEquals(3, c.get(2))
    assertEquals(5, c.get(4))
    assertEquals(6, c.get(5))
    c.put(0, 11)
    assertEquals(11, c.get(0))
    assertEquals(6, c.limit())
    assertEquals(6, c.remaining())
    assertEquals(0, c.position())

    val x = ByteBuffer.wrap("Message".getBytes(StandardCharsets.UTF_8))
    var buffer = ByteBufferUtil.concatenate(Seq(x))

    /**
      * flip() in the ByteBufferUtil causes limit to be set to current position and position to be set to 0
      */
    assertEquals(7, buffer.limit())
    assertEquals(0, buffer.position())
    assertEquals(7, buffer.capacity())
    assertEquals(7, buffer.remaining())

    val y = ByteBuffer.wrap("Nautilus".getBytes(StandardCharsets.UTF_8))
    val z = ByteBuffer.wrap("Pravega".getBytes(StandardCharsets.UTF_8))
    buffer = ByteBufferUtil.concatenate(Seq(y, z))

    assertEquals(15, buffer.remaining())
    assertEquals(15, buffer.capacity())
    assertEquals(0, buffer.position())
    assertEquals(15, buffer.limit())
    assertEquals(78, buffer.get(0))
    assertEquals(97, buffer.get(1))

  }
}
