package io.pravega.connectors.spark

import io.pravega.client.stream.ScalingPolicy.ScalingPolicyBuilder
import org.scalatest.FunSuite

class PravegaSourceProviderTest extends FunSuite {

  // tests: set scaling as fixed, events, bytes per sec

  test("create fixed rate stream") {
    val map = Map("default_num_segments" -> "3")

    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
  }

  test("create KBS stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_bytes_per_sec" -> "10000")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "BY_RATE_IN_KBYTES_PER_SEC")
  }

  test("Create events per sec stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_events_per_sec" -> "20")
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "BY_RATE_IN_EVENTS_PER_SEC")
  }

  test("test empty input to stream config builder") {
    val map = Map[String, String]()
    assert(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType.name() == "FIXED_NUM_SEGMENTS")
  }
}
