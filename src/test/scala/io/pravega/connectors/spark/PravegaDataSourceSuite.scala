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

import java.util.concurrent.atomic.AtomicInteger

import io.pravega.client.stream.StreamCut
import io.pravega.connectors.spark.PravegaSourceProvider._
import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.test.SharedSparkSession


class PravegaDataSourceSuite extends QueryTest with SharedSparkSession with PravegaTest {
  import testImplicits._

  private val streamNumber = new AtomicInteger(0)

  private var testUtils: PravegaTestUtils = _

  private def newStreamName(): String = s"stream${streamNumber.getAndIncrement()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new PravegaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  private def createDF(
      streamName: String,
      withOptions: Map[String, String] = Map.empty[String, String],
      controllerUri: Option[String] = None) = {
    val df = spark
      .read
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, controllerUri.getOrElse(testUtils.controllerUri))
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(event AS STRING)")
  }

  test("explicit earliest to latest offsets") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 3)
    testUtils.sendMessages(streamName, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(streamName, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(streamName, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    val df = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> STREAM_CUT_EARLIEST,
        END_STREAM_CUT_OPTION_KEY -> STREAM_CUT_LATEST))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // "latest" should now bind to the current (latest) offset in a new dataframe.
    testUtils.sendMessages(streamName, (21 to 29).map(_.toString).toArray, Some(2))
    val df2 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> STREAM_CUT_EARLIEST,
        END_STREAM_CUT_OPTION_KEY -> STREAM_CUT_LATEST))
    checkAnswer(df2, (0 to 29).map(_.toString).toDF)
  }

  test("default starting and ending offsets") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 3)
    testUtils.sendMessages(streamName, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(streamName, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(streamName, Array("20"), Some(2))

    // Implicit offset values, should default to earliest and latest
    val df = createDF(streamName)
    // Test that we default to "earliest" and "latest"
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }

  test("explicit stream cuts") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 3)
    testUtils.sendMessages(streamName, (0 to 9).map(_.toString).toArray, Some(0))
    testUtils.sendMessages(streamName, (10 to 19).map(_.toString).toArray, Some(1))
    testUtils.sendMessages(streamName, Array("20"), Some(2))

    val streamInfo1 = testUtils.getStreamInfo(streamName)
    log.info(s"streamInfo1=${streamInfo1}")
    val head1 = streamInfo1.getHeadStreamCut.asText
    val tail1 = streamInfo1.getTailStreamCut.asText
    assert(head1 != tail1)

    // Write more events.
    testUtils.sendMessages(streamName, (21 to 30).map(_.toString).toArray, Some(2))

    // Get new tail stream cut.
    val streamInfo2 = testUtils.getStreamInfo(streamName)
    log.info(s"streamInfo2=${streamInfo2}")
    val head2 = streamInfo2.getHeadStreamCut.asText
    val tail2 = streamInfo2.getTailStreamCut.asText
    assert(head1 == head2)
    assert(tail1 != tail2)
    assert(head2 != tail2)

    // Test explicitly specified stream cuts for head and tail.
    val df = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> head1,
        END_STREAM_CUT_OPTION_KEY -> tail1))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // Reread same dataframe - nothing should change.
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // Create new dataframe with same stream cuts - nothing should change.
    val df2 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> head1,
        END_STREAM_CUT_OPTION_KEY -> tail1))
    checkAnswer(df2, (0 to 20).map(_.toString).toDF)

    // Create new dataframe with entire stream.
    val df3 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> head1,
        END_STREAM_CUT_OPTION_KEY -> tail2))
    checkAnswer(df3, (0 to 30).map(_.toString).toDF)

    // Create new dataframe with only last set of events.
    val df4 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> tail1,
        END_STREAM_CUT_OPTION_KEY -> tail2))
    checkAnswer(df4, (21 to 30).map(_.toString).toDF)

    // Create new dataframe with only last set of events. End uses "latest".
    val df5 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> tail1,
        END_STREAM_CUT_OPTION_KEY -> STREAM_CUT_LATEST))
    checkAnswer(df5, (21 to 30).map(_.toString).toDF)

    // Create new dataframe with only last set of events. Start uses "earliest".
    val df6 = createDF(streamName,
      withOptions = Map(
        START_STREAM_CUT_OPTION_KEY -> STREAM_CUT_EARLIEST,
        END_STREAM_CUT_OPTION_KEY -> tail1))
    checkAnswer(df6, (0 to 20).map(_.toString).toDF)
  }

  test("reuse same dataframe in query") {
    // This test ensures that we do not cache the Pravega reader
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 1)
    testUtils.sendMessages(streamName, (0 to 10).map(_.toString).toArray, Some(0))

    // Specify explicit earliest and latest offset values
    val df = createDF(streamName,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }

  test("metadata") {
    val streamName = newStreamName()
    val df = Seq("1", "2", "3", "4", "5").toDF(EVENT_ATTRIBUTE_NAME)
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(DEFAULT_NUM_SEGMENTS_OPTION_KEY, "5")
      .option(EXACTLY_ONCE, true)
      .mode(SaveMode.Append)
      .save()
    val streamInfo1 = testUtils.getStreamInfo(streamName)

    val metadf = spark
      .read
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(METADATA_OPTION_KEY, MetadataTableName.StreamInfo.toString)
      .load()
    metadf.show(truncate = false)
    val tailStreamCutText = metadf.select("tail_stream_cut").collect().head.getString(0)
    log.info(s"tailStreamCutText=$tailStreamCutText")
    val tailStreamCut = StreamCut.from(tailStreamCutText)
    log.info(s"tailStreamCut=$tailStreamCut")
    assert(tailStreamCut == streamInfo1.getTailStreamCut)
  }

  test("metadata_streams") {
    val streamName1 = newStreamName()
    val streamName2 = newStreamName()
    val streamName3 = newStreamName()
    val streamSet = Set(streamName1, streamName2, streamName3)
    val df = Seq("1", "2", "3", "4", "5").toDF(EVENT_ATTRIBUTE_NAME)
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName1)
      .option(DEFAULT_NUM_SEGMENTS_OPTION_KEY, "5")
      .option(EXACTLY_ONCE, true)
      .mode(SaveMode.Append)
      .save()
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName2)
      .option(DEFAULT_NUM_SEGMENTS_OPTION_KEY, "5")
      .option(EXACTLY_ONCE, true)
      .mode(SaveMode.Append)
      .save()
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName3)
      .option(DEFAULT_NUM_SEGMENTS_OPTION_KEY, "5")
      .option(EXACTLY_ONCE, true)
      .mode(SaveMode.Append)
      .save()

    val metadf = spark
      .read
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName1)
      .option(METADATA_OPTION_KEY, MetadataTableName.Streams.toString)
      .load()
    metadf.show(truncate = false)

    val metaStreams = metadf.select("stream_name").collect().map(_.getString(0))
    assert(metaStreams.contains(streamName1))
    assert(metaStreams.contains(streamName2))
    assert(metaStreams.contains(streamName3))
  }
}
