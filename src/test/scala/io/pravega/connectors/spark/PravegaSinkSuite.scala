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

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import io.pravega.connectors.spark.PravegaReader._
import io.pravega.connectors.spark.PravegaSourceProvider._
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

class PravegaSinkSuite extends StreamTest with SharedSQLContext with PravegaTest {
  import testImplicits._

  protected var testUtils: PravegaTestUtils = _

  override val streamingTimeout: Span = 30.seconds

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

  test("batch - write to Pravega without routing key") {
    val streamName = newStreamName()
    val df = Seq("1", "2", "3", "4", "5").toDF(EVENT_ATTRIBUTE_NAME)
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .save()
    checkAnswer(
      createPravegaReader(streamName).selectExpr("CAST(event as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - write to Pravega with routing key") {
    val streamName = newStreamName()
    val df = Seq("1", "2", "3", "4", "5").map(v => (v, v)).toDF(ROUTING_KEY_ATTRIBUTE_NAME, EVENT_ATTRIBUTE_NAME)
    df.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .save()
    checkAnswer(
      createPravegaReader(streamName).selectExpr("CAST(event as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - unsupported save modes") {
    val streamName = newStreamName()
    val df = Seq("1", "2", "3", "4", "5").toDF(EVENT_ATTRIBUTE_NAME)

    // Test bad save mode Ignore
    var ex = intercept[IllegalArgumentException] {
      df.write
        .format(SOURCE_PROVIDER_NAME)
        .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
        .option(SCOPE_OPTION_KEY, testUtils.scope)
        .option(STREAM_OPTION_KEY, streamName)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains(
      s"save mode ignore not allowed for pravega"))

    // Test bad save mode Overwrite
    ex = intercept[IllegalArgumentException] {
      df.write
        .format(SOURCE_PROVIDER_NAME)
        .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
        .option(SCOPE_OPTION_KEY, testUtils.scope)
        .option(STREAM_OPTION_KEY, streamName)
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains(
      s"save mode overwrite not allowed for pravega"))
  }

  test("SPARK-20496: batch - enforce analyzed plans") {
    val inputEvents =
      spark.range(1, 1000)
        .select(to_json(struct("*")) as 'event)

    val streamName = newStreamName()
    // used to throw UnresolvedException
    inputEvents.write
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .save()
  }

  test("batch - abort transaction") {
    // Based on org/apache/spark/sql/sources/v2/DataSourceV2Suite.scala.
    // This input data will fail to read part way in the 2nd partition.
    val streamName = newStreamName()
    val failingUdf = org.apache.spark.sql.functions.udf {
      var count = 0
      (id: Long, part: Long) => {
        if (part == 1 && count > 5) {
          System.out.println(s"failingUdf: throw exception at id=$id, part=$part")
          // Sleep to hopefully allow partition 0 to call flush on the Pravega transaction.
          Thread.sleep(1000)
          throw new RuntimeException("testing error")
        }
        count += 1
        id
      }
    }
    val input = spark
      .range(100)
      .selectExpr("id", "spark_partition_id() as part")
      .select(failingUdf('id, 'part).as('x))
      .selectExpr("CAST(x as STRING) event")
    val ex = intercept[SparkException] {
      input.write
        .format(SOURCE_PROVIDER_NAME)
        .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
        .option(SCOPE_OPTION_KEY, testUtils.scope)
        .option(STREAM_OPTION_KEY, streamName)
        .save()
      checkAnswer(createPravegaReader(streamName).selectExpr("CAST(event as STRING) value"), Nil)
    }
    // make sure we don't have partial data.
    checkAnswer(createPravegaReader(streamName).selectExpr("CAST(event as STRING) value"), Nil)
    assert(ex.getMessage.contains("Writing job failed"))
}


  test("streaming - write to Pravega without routing key") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    val writer = createPravegaWriter(
      input.toDF(),
      withStreamName = Some(streamName),
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"value as ${EVENT_ATTRIBUTE_NAME}")

    def reader = createPravegaReader(streamName)
      .selectExpr(s"CAST(CAST(${EVENT_FIELD_NAME} as STRING) as INT) event")
      .as[Int]

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write to Pravega without read-after-write consistency") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    val writer = createPravegaWriter(
      input.toDF(),
      withStreamName = Some(streamName),
      withOutputMode = Some(OutputMode.Append),
      withOptions = Map(
        PravegaSourceProvider.READ_AFTER_WRITE_CONSISTENCY_OPTION_KEY -> "false"))(
      withSelectExpr = s"value as ${EVENT_ATTRIBUTE_NAME}")

    def reader = createPravegaReader(streamName)
      .selectExpr(s"CAST(CAST(${EVENT_FIELD_NAME} as STRING) as INT) event")
      .as[Int]

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      // Since we disabled read-after-write consistency, we must reread until we get the expected result.
      eventually(Timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      }
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      eventually(Timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - write to Pravega with routing key") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    val writer = createPravegaWriter(
      input.toDF(),
      withStreamName = Some(streamName),
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"value as ${EVENT_ATTRIBUTE_NAME}", s"value as ${ROUTING_KEY_ATTRIBUTE_NAME}")

    def reader = createPravegaReader(streamName)
      .selectExpr(s"CAST(CAST(${EVENT_FIELD_NAME} as STRING) as INT) event")
      .as[Int]

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write aggregation w/o streamName field, with streamName option") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    // Pravega does not provide separate key and value fields.
    // To store key/value pairs, we encode them as JSON.
    val writer = createPravegaWriter(
      input.toDF().groupBy("value").count(),
      withStreamName = Some(streamName),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr =
        s"TO_JSON(MAP('key', value, 'value', count)) as ${EVENT_ATTRIBUTE_NAME}",
        s"value as ${ROUTING_KEY_ATTRIBUTE_NAME}")

    def reader = createPravegaReader(streamName)
      .selectExpr(s"JSON_TUPLE(CAST(${EVENT_FIELD_NAME} as STRING), 'key', 'value') as (key,value)")
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))
      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }

  test("streaming - write data with bad schema") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* No event field */
      ex = intercept[StreamingQueryException] {
        writer = createPravegaWriter(input.toDF(),
          withStreamName = Some(streamName))(
          withSelectExpr = "value as unused"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains(
      s"required attribute '${EVENT_ATTRIBUTE_NAME}' not found"))
  }

  test("streaming - write data with valid schema but wrong types") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* event field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createPravegaWriter(input.toDF(),
          withStreamName = Some(streamName))(
          withSelectExpr = s"CAST(value as INT) as ${EVENT_ATTRIBUTE_NAME}"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains(
      s"${EVENT_ATTRIBUTE_NAME} attribute type must be a string or binary"))

    try {
      ex = intercept[StreamingQueryException] {
        /* routing key field wrong type */
        writer = createPravegaWriter(input.toDF(),
          withStreamName = Some(streamName))(
          withSelectExpr =
            s"CAST(value as INT) as ${ROUTING_KEY_ATTRIBUTE_NAME}",
            s"value as ${EVENT_ATTRIBUTE_NAME}"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains(
      s"${ROUTING_KEY_ATTRIBUTE_NAME} attribute type must be a string"))
  }

  test("streaming - write to non-existing scope") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createPravegaWriter(input.toDF(),
          withStreamName = Some(streamName),
          withOptions = Map(
            PravegaSourceProvider.ALLOW_CREATE_SCOPE_OPTION_KEY -> "false",
            PravegaSourceProvider.SCOPE_OPTION_KEY -> s"scopefor${streamName}"))(
          withSelectExpr = s"value as ${EVENT_ATTRIBUTE_NAME}")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains("scope does not exist"))
  }

  // This test takes 3 minutes because it must wait for a timeout.
  ignore("streaming - write to non-existing stream") {
    val input = MemoryStream[String]
    val streamName = newStreamName()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createPravegaWriter(input.toDF(),
          withStreamName = Some(streamName),
          withOptions = Map(PravegaSourceProvider.ALLOW_CREATE_STREAM_OPTION_KEY -> "false"))(
          withSelectExpr = s"value as ${EVENT_ATTRIBUTE_NAME}")
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(PravegaTestUtils.exceptionString(ex).toLowerCase(Locale.ROOT).contains("job aborted"))
  }

  private val streamNumber = new AtomicInteger(0)

  private def newStreamName(): String = s"sinkstream${streamNumber.getAndIncrement()}"

  private def createPravegaReader(streamName: String): DataFrame = {
    spark.read
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_EARLIEST)
      .option(END_STREAM_CUT_OPTION_KEY, STREAM_CUT_LATEST)
      .load()
  }

  private def createPravegaWriter(
      input: DataFrame,
      withStreamName: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format(SOURCE_PROVIDER_NAME)
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
        .option(SCOPE_OPTION_KEY, testUtils.scope)
        .queryName("pravegaStream")
      withStreamName.foreach(stream.option(STREAM_OPTION_KEY, _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}
