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

import java.nio.{BufferUnderflowException, ByteBuffer}

import io.pravega.client.batch.SegmentIterator
import org.apache.spark.internal.Logging

import scala.collection.mutable.ArrayBuffer

final case class ChunkDecodeException(private val message: String = "") extends RuntimeException(message)
final case class ChunkSequenceException(private val message: String = "") extends RuntimeException(message)

/**
  * This is a Pravega SegmentIterator that combines adjacent event chunks into super-events.
  *
  * When written to Pravega, chunked events have the following 64-bit header.
  *   version: value must be 1; may be changed for future variants of this encoding (8-bit signed integer)
  *   reserved: padding for 32-bit alignment; value must be 0 (3 8-bit signed integers)
  *   chunk_index: 0-based chunk index (16-bit signed big endian integer)
  *   final_chunk_index: number of chunks minus 1 (16 bit signed big endian integer)
  * Chunks from the same super-event should be written within the same transaction
  * to ensure that they are atomic and contiguous in a segment.
  * Only the chunk_index and final_chunk_index are used to combine chunks.
  * If transactions are not used, chunks may be reassembled incorrectly, causing data corruption.
  */
class ChunkedV1EventIterator(rawIterator: SegmentIterator[ByteBuffer]) extends SegmentIterator[ByteBuffer] with Logging {
  /**
    * The payload (not including chunk headers) for all previous chunks for the current super-event.
    */
  private var buffers: ArrayBuffer[ByteBuffer] = new ArrayBuffer[ByteBuffer]()

  /**
    * The next element that should be returned by next().
    */
  private var nextElement: Option[ByteBuffer] = None

  /**
    * The final_chunk_index for the current super-event.
    */
  private var finalChunkIndex: Short = 0

  /**
    * The offset to the beginning of the first chunk of the current super-event.
    */
  private var firstChunkOffset: Long = 0

  /**
    * If needed, attempt to receive chunks and assemble the next super-event.
    *
    * @return true if there is an available super-chunk to read
    */
  override def hasNext: Boolean = {
    while (nextElement.isEmpty && rawIterator.hasNext) {
      val offset = rawIterator.getOffset
      try {
        val chunkedEvent = ChunkedV1Event.parseEvent(rawIterator.next())
        if (chunkedEvent.chunkIndex == 0) {
          // Handle first chunk.
          if (buffers.nonEmpty) {
            log.warn(s"hasNext: dropping ${buffers.length} buffered chunks because a chunk with index 0 was received")
            buffers.clear()
          }
          buffers += chunkedEvent.chunkedPayload
          finalChunkIndex = chunkedEvent.finalChunkIndex
          firstChunkOffset = offset
        } else if (chunkedEvent.chunkIndex == buffers.length && chunkedEvent.finalChunkIndex == finalChunkIndex) {
          // Handle subsequent chunks.
          buffers += chunkedEvent.chunkedPayload
        } else {
          throw ChunkSequenceException(s"unexpected chunk received: " +
            s"expected chunkIndex=${buffers.length}, received chunkIndex=${chunkedEvent.chunkIndex}, " +
            s"expected finalChunkIndex=${finalChunkIndex}, received finalChunkIndex=${chunkedEvent.finalChunkIndex}")
        }
        // Check if we have the final chunk.
        if (chunkedEvent.chunkIndex == finalChunkIndex) {
          // Assemble buffers. This will be returned by next().
          val assembledBuffer = ByteBufferUtil.concatenate(buffers)
          log.debug(s"hasNext: Assembled ${buffers.length} buffers, ${assembledBuffer.remaining} bytes")
          buffers.clear()
          nextElement = Some(assembledBuffer)
        }
      } catch {
        case e @ (_: BufferUnderflowException | _: ChunkDecodeException | _: ChunkSequenceException) => {
          log.warn("hasNext: Invalid chunk", e)
          buffers.clear()
        }

      }
    }
    nextElement.isDefined
  }

  /**
    * @return Returns the current super-event.
    */
  override def next(): ByteBuffer = {
    if (hasNext) {
      val toReturn = nextElement.get
      nextElement = None
      toReturn
    } else {
      throw new NoSuchElementException()
    }
  }

  /**
    * @return Returns the offset to the beginning of the first chunk of the current super-event.
    *         The current super-event is the one that will be returned by next().
    */
  override def getOffset: Long = firstChunkOffset

  override def close(): Unit = {
    rawIterator.close()
  }
}

case class ChunkedV1Event(version: Byte, chunkIndex: Short, finalChunkIndex: Short, chunkedPayload: ByteBuffer)

object ChunkedV1Event {
  def parseEvent(event: ByteBuffer): ChunkedV1Event = {
    val version = event.get
    if (version == 1) {
      // Skip 3 bytes of padding
      event.get
      event.get
      event.get
      val chunkIndex = event.getShort
      val finalChunkIndex = event.getShort
      if (chunkIndex < 0 || finalChunkIndex < 0 || finalChunkIndex < chunkIndex) {
        throw ChunkDecodeException(s"Invalid chunkIndex (${chunkIndex}) or finalChunkIndex (${finalChunkIndex})")
      }
      ChunkedV1Event(version, chunkIndex, finalChunkIndex, event)
    } else {
      throw ChunkDecodeException(s"Unknown chunked event version ${version}")
    }
  }
}
