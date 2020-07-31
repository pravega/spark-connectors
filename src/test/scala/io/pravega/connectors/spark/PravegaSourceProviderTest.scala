package io.pravega.connectors.spark

import io.pravega.client.stream.ScalingPolicy.ScalingPolicyBuilder
import org.scalatest.FunSuite

class PravegaSourceProviderTest extends FunSuite {

  // tests: set scaling as fixed, events, bytes per sec

  test("create fixed rate stream") {
    val map = Map("default_num_segments" -> "3")

    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getMinNumSegments == 3)
  }

  test("set retention policy by duration") {
    val map = Map("default_retention_duration_milliseconds" -> "1000")

    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
  }

  test("set retention policy by size in bytes") {
    val map = Map("default_retention_size_bytes" -> "5000")

    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
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

}
