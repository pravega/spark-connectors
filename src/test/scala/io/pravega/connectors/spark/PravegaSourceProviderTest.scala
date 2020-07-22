package io.pravega.connectors.spark

import org.scalatest.FunSuite

class PravegaSourceProviderTest extends FunSuite {

  // tests: set scaling as fixed, events, bytes per sec

  test("create fixed rate stream") {
    val map = Map("default_num_segments" -> "3")

    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
  }

  test("create KBS stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_bytes_per_sec" -> "10000")
    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
  }

  test("Create events per sec stream") {
    val map = Map("default_num_segments" -> "3"
      , "default_scale_factor" -> "2"
      , "default_segment_target_rate_events_per_sec" -> "20")
    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
  }

  test("test empty input to stream config builder") {
    val map = Map[String, String]()
    print(PravegaSourceProvider.buildStreamConfig(map).getScalingPolicy.getScaleType)
  }
}
