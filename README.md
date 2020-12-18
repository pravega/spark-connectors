<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Spark Connectors [![Build Status](https://travis-ci.org/pravega/spark-connectors.svg?branch=master)](https://travis-ci.org/pravega/spark-connectors) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

This repository implements connectors to read and write [Pravega](http://pravega.io/) Streams
with [Apache Spark](http://spark.apache.org/),
a high-performance analytics engine for batch and streaming data.

The connectors can be used to build end-to-end stream processing pipelines
(see [Samples](https://github.com/pravega/pravega-samples))
that use Pravega as the stream storage and message bus, and Apache Spark for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**

  - A Spark micro-batch reader connector allows Spark streaming applications to read Pravega Streams.
    Pravega stream cuts (i.e. offsets) are used to reliably recover from failures and provide exactly-once semantics.
    
  - A Spark batch reader connector allows Spark batch applications to read Pravega Streams.

  - A Spark writer allows Spark batch and streaming applications to write to Pravega Streams.
    Writes are optionally contained within Pravega transactions, providing exactly-once semantics.

  - Seamless integration with Spark's checkpoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.
  
## Compatibility Matrix

The [master](https://github.com/pravega/spark-connectors) branch will always have the most recent
supported versions of Spark and Pravega.

| Spark Version | Pravega Version | Git Branch                                                                        |
|---------------|-----------------|-----------------------------------------------------------------------------------|
| 3.0           | 0.8             | [master](https://github.com/pravega/spark-connectors)                             |
| 2.4           | 0.8             | [r0.8-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.8-spark2.4)   |

## Prerequisites

The following prerequisites are required for building and running in production:

- Java 8

## Build and Install the Spark Connector

This will build the Spark Connector and publish it to your local Maven repository.

```
$ git clone -b [BRANCH_NAME] https://github.com/pravega/spark-connectors
$ cd spark-connectors
$ ./gradlew install
$ ls -lhR ~/.m2/repository/io/pravega/
```

## Download the Pre-Built Artifacts.
The pre-built artifacts are published in our [JFrog repository](http://oss.jfrog.org/jfrog-dependencies/io/pravega/)


## Configuration

The following table lists the configurable parameters of the Pravega Spark connector and their default values.

| Parameter | Description | Default |
| ----- | ----------- | ------ |
| `allow_create_scope` | When turned on, Pravega scope will be automatically created. Only enable this if Pravega is running in stand-alone mode. | `true` |
| `allow_create_stream` | When turned on, Pravega stream will be automatically created. | `true` |
| `controller` |  The URI endpoint of the Pravega controller in the form of `protocol://<hostname/ip>:9090`.| `tcp://localhost:9090` |
| `default_num_segments` | The default number of segments for a stream. | |
| `default_retention_duration_milliseconds` | The default time in form of `ms` to decide how much data to retain within a stream. |  |
| `default_retention_size_bytes` | The default size in the form of `bytes` to decide how much data to retain within a stream.  | |
| `default_scale_factor` | The default scale factor for a Stream to decide if it should automatically scale its number of segments.   |  |
| `default_segment_target_rate_bytes_per_sec` | The target rate for a segment in the form of `bytes` per second.| |
| `default_segment_target_rate_events_per_sec` | The target rate for a segment in the form of `events` per second.| |
| `end_stream_cut` | The end offset of a stream. | Batch Job: `latest`; Stream Job: `unbounded` |
| `exactly_once` | Pravega with transaction enabled (exactly-once semantics). | `true` |
| `metadata` | The metadata reader for getting the a stream(`StreamInfo`) or a scope info(`Streams`).| |
| `read_after_write_consistency` | Pravega with transaction enabled (exactly-once semantics) in the streaming job. | `true` |
| `scope` | The Pravega scope containing the data stream. |  |
| `start_stream_cut` | The start offset of a stream. | Batch Job: `earliest`; Stream Job: `latest` |
| `stream` | The name of the data stream to read or write. |  |
| `transaction_timeout_ms` | The time-out value for a transaction in the form of `ms` | `30 * 1000` |
| `transaction_status_poll_interval_ms` |  The time interval in `ms` for which the transaction status has to be pulled  | `50` |


## Examples

To learn more about how to build and use the Spark Connector library with [Pravega](https://www.pravega.io/), refer to
[Pravega Samples](https://github.com/pravega/pravega-samples/tree/master/spark-connector-examples).

To learn more about how to build and use the Spark Connector library with [Dell EMC Streaming Data Platform](https://www.delltechnologies.com/en-us/storage/streaming-data-platform.htm), refer to
[Workshop Samples](https://github.com/StreamingDataPlatform/workshop-samples/tree/master/spark-examples).

## Limitations

  - This connector does *not* guarantee that events with the same routing key
    are returned in a single partition. 
    If your application requires this, you must repartition the dataframe by the routing key and sort within the
    partition by segment_id and offset.

  - Continuous reader support is not available. The micro-batch reader uses the Pravega batch API and works well for
    applications with latency requirements above 100 milliseconds.

  - The initial batch in the micro-batch reader will contain the entire Pravega stream as of the start time.
    There is no rate limiting functionality.

  - Read-after-write consistency is currently *not* guaranteed.
    Be cautious if your workflow requires multiple chained Spark batch jobs.

## About

Spark connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.
