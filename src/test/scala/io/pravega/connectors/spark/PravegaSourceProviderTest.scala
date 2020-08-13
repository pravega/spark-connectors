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

import org.scalatest.FunSuite

class PravegaSourceProviderTest extends FunSuite {

  // test scaling policy
  test("create fixed rate stream") {
    val map = Map("default_num_segments" -> "3")

    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getMinNumSegments == 3)
  }

  test("create KBS stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_bytes_per_sec" -> "1024")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "BY_RATE_IN_KBYTES_PER_SEC")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getMinNumSegments == 3)
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleFactor == 2)
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getTargetRate == 1)

  }

  test("Create events per sec stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_events_per_sec" -> "20")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "BY_RATE_IN_EVENTS_PER_SEC")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getMinNumSegments == 3)
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleFactor == 2)
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getTargetRate == 20)

  }

  test("test empty input to stream config builder") {
    val map = Map[String, String]()
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
  }

  // test retention policy
  test("set retention policy by duration") {
    val map = Map("default_retention_duration_milliseconds" -> "1000")

    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
    assert(PravegaSourceProvider.buildStreamConfig(map).getRetentionPolicy.getRetentionType.name() == "TIME")
    assert(PravegaSourceProvider.buildStreamConfig(map).getRetentionPolicy.getRetentionParam == 1000)
  }

  test("set retention policy by size in bytes") {
    val map = Map("default_retention_size_bytes" -> "5000")

    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
    assert(PravegaSourceProvider.buildStreamConfig(map).getRetentionPolicy.getRetentionType.name() == "SIZE")
    assert(PravegaSourceProvider.buildStreamConfig(map).getRetentionPolicy.getRetentionParam == 5000)

  }
}
