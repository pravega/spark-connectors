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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger
import io.pravega.client.stream.StreamCut
import io.pravega.connectors.spark.PravegaSourceProvider._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, SparkDataStream}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import scala.util.Random

abstract class PravegaSourceTest extends StreamTest with SharedSparkSession with PravegaTest {

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
    }
    super.afterAll()
  }

  protected def makeSureGetOffsetCalled = AssertOnQuery { q =>
    // Because PravegaSource's initialPartitionOffsets is set lazily, we need to make sure
    // its "getOffset" is called before pushing any data. Otherwise, because of the race condition,
    // we don't know which data should be fetched when `startingOffsets` is latest.
    q match {
      case m: MicroBatchExecution => m.processAllAvailable()
    }
    true
  }

  protected def setStreamSegments(streamName: String, numSegments: Int, query: StreamExecution) : Unit = {
    testUtils.setStreamSegments(streamName, numSegments)
  }

  /**
    * Add data to Pravega.
    *
    * `topicAction` can be used to run actions for each topic before inserting data.
    */
  case class AddPravegaData(streamNames: Set[String], data: Int*)
                         (implicit ensureDataInMultiplePartition: Boolean = false,
                          concurrent: Boolean = false,
                          message: String = "",
                          numSegments: Option[Int] = None) extends AddData {

    override def addData(query: Option[StreamExecution]): (SparkDataStream, Offset) = {
      query match {
        // Make sure no Spark job is running when deleting a streamName
        case Some(m: MicroBatchExecution) => m.processAllAvailable()
        case _ =>
      }

      numSegments.foreach(n => streamNames.foreach(s => testUtils.setStreamSegments(s, n)))

      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active pravega source")

      val sources = {
        query.get.logicalPlan.collect {
          case StreamingExecutionRelation(source: PravegaMicroBatchStream, _, _) => source
          case r: StreamingDataSourceV2Relation if r.stream.isInstanceOf[PravegaMicroBatchStream] =>
            r.stream
        }
      }.distinct

      if (sources.isEmpty) {
        throw new Exception(
          "Could not find Pravega source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the Pravega source in the StreamExecution logical plan as there" +
            "are multiple Pravega sources:\n\t" + sources.mkString("\n\t"))
      }
      val pravegaSource = sources.head
      val streamName = streamNames.toSeq(Random.nextInt(streamNames.size))
      testUtils.sendMessages(streamName, data.map { _.toString }.toArray)
      val offset = PravegaSourceOffset(testUtils.getLatestStreamCut(streamNames))
      logInfo(s"Added data, expected offset $offset")
      (pravegaSource, offset)
    }

    override def toString: String =
      s"AddPravegaData(streamNames = $streamNames, data = $data, message = $message)"
  }

  private val streamNumber = new AtomicInteger(0)

  protected def newStreamName(): String = s"stream${streamNumber.getAndIncrement()}"
}

abstract class PravegaSourceSuiteBase extends PravegaSourceTest {

  import testImplicits._

  private def waitUntilBatchProcessed(clock: StreamManualClock) = AssertOnQuery {
    q =>
    eventually(Timeout(streamingTimeout)) {
      if (!q.exception.isDefined) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      throw q.exception.get
    }
    true
  }

  test("stop stream before reading anything") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 5)
    testUtils.sendMessages(streamName, (101 to 105).map { _.toString }.toArray)

    val reader = spark
      .readStream
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)

    val dataset = reader.load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]
    val mapped = dataset.map(e => e.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  test(s"read from latest stream cut") {
    val streamName = newStreamName()
    testFromLatestStreamCut(
      streamName,
      addSegments = false)
  }

  test(s"read from latest stream cut with maxoffset 1") {
    val streamName = newStreamName()
    testFromLatestStreamCut(
      streamName,
      addSegments = false,
      (MAX_OFFSET_PER_TRIGGER, "1"))
  }

  test(s"read from latest stream cut with maxoffset 1000") {
    val streamName = newStreamName()
    testFromLatestStreamCut(
      streamName,
      addSegments = false,
      (MAX_OFFSET_PER_TRIGGER, "1000"))
  }

  test(s"read from latest stream cut with maxoffset 10") {
    val streamName = newStreamName()
    testFromLatestStreamCut(
      streamName,
      addSegments = false,
      (MAX_OFFSET_PER_TRIGGER, "10"))
  }

  test(s"read from earliest stream cut") {
    val streamName = newStreamName()
    testFromEarliestStreamCut(
      streamName,
      addSegments = false)
  }

  test(s"read from earliest stream cut with maxoffset 10") {
    val streamName = newStreamName()
    testFromEarliestStreamCut(
      streamName,
      addSegments = false,
      5,
      Map("MAX_OFFSET_PER_TRIGGE" -> "10")
    )
  }

  test(s"read from earliest stream cut with maxoffset 1000") {
    val streamName = newStreamName()
    testFromEarliestStreamCut(
      streamName,
      addSegments = false,
      5,
      Map("MAX_OFFSET_PER_TRIGGE" -> "1000")
    )
  }

  test(s"read from specific stream cut") {
    val streamName = newStreamName()
    testFromSpecificStreamCut(
      streamName,
      addSegments = false)
  }

  test(s"read from specific stream cut with maxoffset 10") {
    val streamName = newStreamName()
    testFromSpecificStreamCut(
      streamName,
      addSegments = false,
      (MAX_OFFSET_PER_TRIGGER, "10"))
  }

  test(s"read from earliest stream cut, add new segments") {
    val streamName = newStreamName()
    testFromEarliestStreamCut(
      streamName,
      numSegments = 3,
      addSegments = true)
  }

  test("get offsets from case insensitive parameters") {
    val streamCutText = "H4sIAAAAAAAAADOwKk7OL0jVLy4pSk3MNbIy1DHSMdAx1jGxMtCBQiDLUsfQAkwAANe6Pt8vAAAA"
    for ((optionKey, optionValue, answer) <- Seq(
      (START_STREAM_CUT_OPTION_KEY, "earLiEst", EarliestStreamCut),
      (END_STREAM_CUT_OPTION_KEY, "laTest", LatestStreamCut),
      (START_STREAM_CUT_OPTION_KEY, streamCutText, SpecificStreamCut(StreamCut.from(streamCutText))))) {
      val offset = getPravegaStreamCut(Map(optionKey -> optionValue), optionKey, answer)
      assert(offset === answer)
    }

    for ((optionKey, answer) <- Seq(
      (START_STREAM_CUT_OPTION_KEY, EarliestStreamCut),
      (END_STREAM_CUT_OPTION_KEY, UnboundedStreamCut))) {
      val offset = getPravegaStreamCut(Map.empty, optionKey, answer)
      assert(offset === answer)
    }
  }

  test(s"read in batches of maxoffset 10") {
    val streamName = newStreamName()
    testForBatchSizeMinimum(getDataSet(
      streamName,
      addSegments = false,
      numSegments = 3,
      (MAX_OFFSET_PER_TRIGGER, "10")))
  }

  // ApproxDistance per segemnt(1/3=0.3) next StreamCut is fetched i.e 1 which is less then each event length i.e 10 . batch 1 will yield one event each
  test(s"read in batches of maxoffset 1")
  {
    val streamName = newStreamName()
    testForBatchSizeMinimum(getDataSet(
      streamName,
      addSegments = false,
      numSegments = 3,
      (MAX_OFFSET_PER_TRIGGER, "1")))
  }

  // ApproxDistance per segemnt(30/3=10) equal to each event length i.e 10 . batch 1 will yield one event each
  test(s"read in batches of maxoffset 30") {
    val streamName = newStreamName()
    testForBatchSizeMinimum(getDataSet(
      streamName,
      addSegments = false,
      numSegments = 3,
      (MAX_OFFSET_PER_TRIGGER, "30")))
  }

  // ApproxDistance per segemnt(33/3=11) greater then each event length i.e 10 . batch 1 will yield two events each
  // nextStreamCut = scope/stream0:0=20, 1=20, 2=20 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
  test(s"read in batches of maxoffset 33") {
    val streamName = newStreamName()
    testForBatchSizeCustom(getDataSet(
      streamName,
      addSegments = false,
      numSegments = 3,
      (MAX_OFFSET_PER_TRIGGER, "33")))
  }
  // ApproxDistance per segemnt(100/3=33) greater then each event length of all 3 events i.e 30 . batch 1 will yield all events till tail.
  //nextStreamCut = scope/stream0:0=30, 1=30, 2=30 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
  test(s"read in batches of maxoffset 100") {
    val streamName = newStreamName()
    testForBatchSizeTooLarge(getDataSet(
      streamName,
      addSegments = false,
      numSegments = 3,
      (MAX_OFFSET_PER_TRIGGER, "100")))
  }

  private def getDataSet(
                   streamName: String,
                   addSegments: Boolean,
                   numSegments: Int,
                   options: (String, String)*): Dataset[Int] = {
    testUtils.createTestStream(streamName, numSegments = numSegments)
    testUtils.sendMessages(streamName, Array(10, 40, 70).map(_.toString), Some(0)) // appends 10,40,70 to segment 0 each length is 10
    testUtils.sendMessages(streamName, Array(20, 50, 80).map(_.toString), Some(1)) // appends 20,50,80 to segment 0 each length is 10
    testUtils.sendMessages(streamName, Array(30, 60, 90).map(_.toString), Some(2)) // appends 30,60,90 to segment 0 each length is 10
    require(testUtils.getLatestStreamCut(Set(streamName)).asImpl().getPositions.size === numSegments)

    val reader = spark.readStream
    reader
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_EARLIEST)
    options.foreach
    { case (k, v) => reader.option(k, v) }
    val dataset = reader.load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]
    dataset.map(e => e.toInt + 1)
  }

  /*
    *  Batch 1: nextStreamCut = scope/stream0:0=10, 1=10, 2=10 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
    *  Batch 2: nextStreamCut = scope/stream0:0=20, 1=20, 2=20 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
    *  Batch 3: nextStreamCut = scope/stream0:0=30, 1=30, 2=30 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
    */
  private def testForBatchSizeMinimum( mapped: Dataset[Int]): Unit = {
    val clock = new StreamManualClock
    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61, 71, 81, 91)
    )
  }

  /*
    *  Batch 1: nextStreamCut = scope/stream0:0=20, 1=20, 2=20 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
    *  Batch 2: nextStreamCut = scope/stream0:0=30, 1=30, 2=30 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
    */
  private def testForBatchSizeCustom(mapped: Dataset[Int]): Unit = {
    val clock = new StreamManualClock
    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61, 71, 81, 91)
    )
  }

  /*
   *  Batch 1: nextStreamCut = scope/stream0:0=30, 1=30, 2=30 Tail stream cut = scope/stream0:0=30, 1=30, 2=30
   */
  private def testForBatchSizeTooLarge(mapped: Dataset[Int]): Unit = {
    val clock = new StreamManualClock
    testStream(mapped)(
      StartStream(Trigger.ProcessingTime(100), clock),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61, 71, 81, 91),
      AdvanceManualClock(100),
      waitUntilBatchProcessed(clock),
      CheckAnswer(11, 21, 31, 41, 51, 61, 71, 81, 91)
    )
  }

  private def testFromLatestStreamCut(
                                     streamName: String,
                                     addSegments: Boolean,
                                     options: (String, String)*): Unit = {
    testUtils.createTestStream(streamName, numSegments = 5)
    testUtils.sendMessages(streamName, Array("-1"))
    require(testUtils.getLatestStreamCut(Set(streamName)).asImpl().getPositions.size === 5)

    val reader = spark.readStream
    reader
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_LATEST)
    options.foreach { case (k, v) => reader.option(k, v) }
    val dataset = reader.load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]
    val mapped = dataset.map(e => e.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddPravegaData(Set(streamName), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddPravegaData(Set(streamName), 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddPravegaData(Set(streamName), 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add segments") { query: StreamExecution =>
        if (addSegments) {
          setStreamSegments(streamName, 10, query)
        }
        true
      },
      AddPravegaData(Set(streamName), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestStreamCut(
                                       streamName: String,
                                       addSegments: Boolean,
                                       numSegments: Int  = 5,
                                       options: Map[String, String] = Map[String, String]()): Unit = {
    testUtils.createTestStream(streamName, numSegments = numSegments)
    testUtils.sendMessages(streamName, (1 to 3).map { _.toString }.toArray)
    require(testUtils.getLatestStreamCut(Set(streamName)).asImpl().getPositions.size === numSegments)

    val reader = spark.readStream
    reader
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_EARLIEST)
    options.foreach { case (k, v) => reader.option(k, v) }
    val dataset = reader.load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]
    val mapped = dataset.map(e => e.toInt + 1)

    testStream(mapped)(
      AddPravegaData(Set(streamName), 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddPravegaData(Set(streamName), 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add segments") { query: StreamExecution =>
        if (addSegments) {
          setStreamSegments(streamName, numSegments * 2, query)
        }
        true
      },
      AddPravegaData(Set(streamName), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromSpecificStreamCut(
                                       streamName: String,
                                       addSegments: Boolean,
                                       options: (String, String)*): Unit = {
    testUtils.createTestStream(streamName, numSegments = 5)

    // Following messages are before the stream cut and should be skipped.
    testUtils.sendMessages(streamName, Array(-10, -11, -12).map(_.toString), Some(1))
    testUtils.sendMessages(streamName, Array(10).map(_.toString), Some(3))
    testUtils.sendMessages(streamName, Array(20, 21).map(_.toString), Some(4))

    val streamInfo1 = testUtils.getStreamInfo(streamName)
    log.info(s"testFromSpecificStreamCut: streamInfo1=${streamInfo1}")
    val head1 = streamInfo1.getHeadStreamCut.asText
    val tail1 = streamInfo1.getTailStreamCut.asText
    assert(head1 != tail1)

    // Following messages are after the stream cut and should be seen.
    testUtils.sendMessages(streamName, Array(-20, -21, -22).map(_.toString), Some(0))
    testUtils.sendMessages(streamName, Array(0, 1, 2).map(_.toString), Some(2))
    testUtils.sendMessages(streamName, Array(11, 12).map(_.toString), Some(3))
    testUtils.sendMessages(streamName, Array(22).map(_.toString), Some(4))

    val reader = spark.readStream
    reader
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, tail1)
    options.foreach { case (k, v) => reader.option(k, v) }
    val dataset = reader.load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]
    val mapped = dataset.map(e => e.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22),
      StopStream,
      StartStream(),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22), // Should get the data back on recovery
      AddPravegaData(Set(streamName), 30, 31, 32, 33, 34),
      CheckAnswer(-20, -21, -22, 0, 1, 2, 11, 12, 22, 30, 31, 32, 33, 34),
      StopStream
    )
  }

  test("Pravega column types") {
    val now = System.currentTimeMillis()
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 1)
    testUtils.sendMessages(streamName, Array(1).map(_.toString))

    val df = spark
      .readStream
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_EARLIEST)
      .load()

    val query = df
      .writeStream
      .format("memory")
      .queryName("columnTypes")
      .trigger(defaultTrigger)
      .start()
    eventually(timeout(streamingTimeout)) {
      assert(spark.table("columnTypes").count == 1,
        s"Unexpected results: ${spark.table("columnTypes").collectAsList()}")
    }
    val row = spark.table("columnTypes").head()
    assert(row.getAs[Array[Byte]](PravegaReader.EVENT_FIELD_NAME) === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String](PravegaReader.STREAM_FIELD_NAME) === streamName, s"Unexpected results: $row")
    assert(row.getAs[String](PravegaReader.SCOPE_FIELD_NAME) === testUtils.scope, s"Unexpected results: $row")
    assert(row.getAs[Int](PravegaReader.SEGMENT_ID_FIELD_NAME) === 0, s"Unexpected results: $row")
    assert(row.getAs[Long](PravegaReader.OFFSET_FIELD_NAME) === 0L, s"Unexpected results: $row")
    query.stop()
  }
}

abstract class PravegaMicroBatchSourceSuiteBase extends PravegaSourceSuiteBase {

  import testImplicits._

  test("(de)serialization of initial offsets") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 5)

    val reader = spark
      .readStream
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)

    testStream(reader.load)(
      makeSureGetOffsetCalled,
      StopStream,
      StartStream(),
      StopStream)
  }

  test("input row metrics") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 5)
    testUtils.sendMessages(streamName, Array("-1"))

    val dataset = spark
      .readStream
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .load()
      .selectExpr("CAST(event AS STRING)")
      .as[String]

    val mapped = dataset.map(e => e.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = Trigger.ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddPravegaData(Set(streamName), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("ensure stream-stream self-join generates only one offset in log and correct metrics") {
    val streamName = newStreamName()
    testUtils.createTestStream(streamName, numSegments = 2)

    val df = spark
      .readStream
      .format(SOURCE_PROVIDER_NAME)
      .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
      .option(SCOPE_OPTION_KEY, testUtils.scope)
      .option(STREAM_OPTION_KEY, streamName)
      .load()

    val values = df
      .selectExpr("CAST(CAST(event AS STRING) AS INT) AS value",
        "CAST(CAST(event AS STRING) AS INT) % 5 AS key")

    val join = values.join(values, "key")

    testStream(join)(
      makeSureGetOffsetCalled,
      AddPravegaData(Set(streamName), 1, 2),
      CheckAnswer((1, 1, 1), (2, 2, 2)),
      AddPravegaData(Set(streamName), 6, 3),
      CheckAnswer((1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 6, 1), (1, 1, 6), (1, 6, 6)),
      AssertOnQuery { q =>
        assert(q.availableOffsets.iterator.size == 1)
        assert(q.recentProgress.map(_.numInputRows).sum == 4)
        true
      }
    )
  }
}

class PravegaMicroBatchV2SourceSuite extends PravegaMicroBatchSourceSuiteBase {
}

class PravegaSourceStressSuite extends PravegaSourceTest {

  import testImplicits._

  val streamNumber = new AtomicInteger(1)

  // create new Random object
  val randomNumberGen = new Random()

  @volatile var streamNames: Seq[String] = (1 to 1).map(_ => newStressStreamName)

  def newStressStreamName: String = s"stress${streamNumber.getAndIncrement()}"

  private def nextInt(start: Int, end: Int): Int = {
    start + randomNumberGen.nextInt(start + end - 1)
  }

  test("stress test with multiple streams and segments")  {
    // Set seed to find random number of segments to scale with
    val seed = 1
    randomNumberGen.setSeed(seed)

    streamNames.foreach { streamName =>
      val numSegments = nextInt(1, 6)
      testUtils.createTestStream(streamName, numSegments = numSegments)
      testUtils.sendMessages(streamName, (101 to 105).map { _.toString }.toArray)
    }

    val streamName = streamNames.head

    // Create Pravega source that reads from latest offset
    val pravega =
      spark.readStream
        .format(SOURCE_PROVIDER_NAME)
        .option(CONTROLLER_OPTION_KEY, testUtils.controllerUri)
        .option(SCOPE_OPTION_KEY, testUtils.scope)
        .option(STREAM_OPTION_KEY, streamName)
        .option(START_STREAM_CUT_OPTION_KEY, STREAM_CUT_LATEST)
        .load()
        .selectExpr("CAST(event AS STRING)")
        .as[String]

    val mapped = pravega.map(e => e.toInt + 1)

    runStressTest(
      mapped,
      Seq(makeSureGetOffsetCalled),
      (d, running) => {
        nextInt(0, 5) match {
          case 0 => // Set number of segments
            AddPravegaData(streamNames.toSet, d: _*)(
              message = "Set number of segments",
              numSegments = Some(nextInt(1, 6)))
          case _ => // Just add new data
            AddPravegaData(streamNames.toSet, d: _*)
        }
      },
      iterations = 50)
  }
}
