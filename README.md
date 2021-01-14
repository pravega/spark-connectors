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
(see [Samples](https://github.com/pravega/pravega-samples/tree/dev/spark-connector-examples))
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

| Spark Version | Pravega Version | Java Version | Git Branch                                                                        |
|---------------|-----------------|--------------|-----------------------------------------------------------------------------------|
| 3.0           | 0.9             | Java 11      | [master](https://github.com/pravega/spark-connectors)                             |
| 3.0           | 0.8             | Java 8       | [r0.8-spark3.0](https://github.com/pravega/spark-connectors/tree/r0.8-spark3.0)   |
| 2.4           | 0.8             | Java 8       | [r0.8-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.8-spark2.4)   |

## Prerequisites

The following prerequisites are required for building and running this connector:

- Java 8 or 11 (see compatibility matrix)

## Examples

To learn how to build and run Spark applications with the Pravega Spark Connectors, refer to
[Pravega Samples](https://github.com/pravega/pravega-samples/tree/dev/spark-connector-examples).

To learn how to build and run Spark applications with [Dell EMC Streaming Data Platform](https://www.delltechnologies.com/en-us/storage/streaming-data-platform.htm), refer to
[Workshop Samples](https://github.com/StreamingDataPlatform/workshop-samples/tree/master/spark-examples).

## Configuration

The following table lists the configurable parameters of the Pravega Spark connector and their default values.

| Parameter | Description | Default |
| ----- | ----------- | ------ |
| `allow_create_scope` | If true, the Pravega scope will be automatically created. This must be false when running in Dell EMC Streaming Data Platform. | `true` |
| `allow_create_stream` | If true, the Pravega stream will be automatically created. | `true` |
| `controller` |  The URI endpoint of the Pravega controller in the form of `protocol://hostname:port`.| `tcp://localhost:9090` |
| `default_num_segments` | The default number of segments for a stream. This is ignored if the stream already exists. | |
| `default_retention_duration_milliseconds` | The default time in form of `ms` to decide how much data to retain within a stream. This is ignored if the stream already exists. |  |
| `default_retention_size_bytes` | The default size in the form of `bytes` to decide how much data to retain within a stream. This is ignored if the stream already exists.  | |
| `default_scale_factor` | The default scale factor for a stream to decide if it should automatically scale its number of segments. This is ignored if the stream already exists.  |  |
| `default_segment_target_rate_bytes_per_sec` | The target rate for a segment in the form of `bytes` per second. This is ignored if the stream already exists. | |
| `default_segment_target_rate_events_per_sec` | The target rate for a segment in the form of `events` per second. This is ignored if the stream already exists. | |
| `end_stream_cut` | The end stream cut (offsets) of a stream. Can be `latest`, `unbounded`, or a specific base-64 encoded stream cut. `latest` will be resolved at the start of the job. | Batch Job: `latest`; Stream Job: `unbounded` |
| `exactly_once` | If true, use Pravega transactions when writing to provide exactly-once semantics. Set to false for reduced write latency. | `true` |
| `metadata` | If set, a read will return scope or stream metadata instead of the stream data. "Streams" will return a list of streams in the scope. "StreamInfo" will return head and tail stream cuts.| |
| `read_after_write_consistency` | If true, commits will wait for Pravega to finish committing transactions before completing. | `true` |
| `scope` | The Pravega scope containing the data stream. |  |
| `start_stream_cut` | The start stream cut (offsets) of a stream. Can be `earliest`, `latest`, `unbounded`, or a specific base-64 encoded stream cut. `earliest` and `latest` will be resolved at the start of the job. | Batch Job: `earliest`; Stream Job: `latest` |
| `stream` | The name of the Pravega stream to read or write. |  |
| `transaction_timeout_ms` | The time-out value for a transaction in the form of `ms`. | Batch Job: `120000` (2 minutes); Stream Job: `30000` |
| `transaction_status_poll_interval_ms` |  The time interval in `ms` for which the transaction status is polled. This is used only if `read_after_write_consistency` is true. | `50` |

## Configuring Pravega for Exactly-Once

When writing events to Pravega with `exactly_once` set to true (the default), Pravega transactions are used.
This connector begins a transaction during the execution of each task and commits all transactions
only at the end of the batch job or at the end of the micro-batch.
To prevent the Pravega transaction from timing out, you must apply the following configuration.

1. The Pravega Controller must be configured with the following settings:
```
    pravega-cluster:
      pravega_options:
        controller.transaction.maxLeaseValue: "2147483647"    # 24.8 days
        controller.transaction.ttlHours: "720"                # 30 days
```

2. The Pravega Spark connector writer must be configured with a reasonable value for `transaction_timeout_ms`.
   This should be the maximum number of milliseconds that you expect the Spark job to take.
   The maximum value that is known to work is 64800000 ms (18 hours).

## Build and Install the Pravega Spark Connectors

This will build the Pravega Spark Connectors and publish it to your local Maven repository.

```shell script
$ git clone https://github.com/pravega/spark-connectors
$ cd spark-connectors
$ git checkout $BRANCH   # optional
$ ./gradlew install
$ ls -lhR ~/.m2/repository/io/pravega/
```

## Pre-Built Artifacts

The pre-built artifacts are published in our [JFrog repository](http://oss.jfrog.org/jfrog-dependencies/io/pravega/).

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
