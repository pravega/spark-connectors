<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Pravega Spark Connectors

This repository implements connectors to read and write [Pravega](http://pravega.io/) Streams
with [Apache Spark](http://spark.apache.org/),
a high-performance analytics engine for batch and streaming data.

The connectors can be used to build end-to-end stream processing pipelines
(see [Samples](https://github.com/pravega/pravega-samples))
that use Pravega as the stream storage and message bus, and Apache Spark for computation over the streams.


## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**

  - A Spark V2 data source micro-batch reader connector allows Spark Streaming applications to read Pravega Streams.
    Pravega stream cuts are used to reliably recover from failures and provide exactly-once semantics.

  - A Spark V2 data source stream writer allows Spark Streaming applications to write to Pravega Streams.
    Writes are contained within Pravega transactions, providing exactly-once semantics.

  - Seamless integration with Spark's checkpoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.

## Limitations

  - Continuous reader support is not available. The micro-batch reader uses the Pravega batch API and works well for
    applications with latency requirements above 100 milliseconds.

  - The initial batch in the micro-batch reader will contain the entire Pravega stream as of the start time.
    There is no rate limiting functionality.

  - This connector has not been tested with Spark batch application.
    However, the Hadoop connector can be used for this.

  - Pravega [issue 3463](https://github.com/pravega/pravega/issues/3463) impacts long-running
    Spark Streaming jobs that write to Pravega.

## Documentation

To learn more about how to build and use the Spark Connector library, refer to
[Pravega Samples](https://github.com/claudiofahey/pravega-samples/tree/spark-connector-examples).

## About

Spark connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.

## Reference

- http://blog.madhukaraphatak.com/spark-datasource-v2-part-1/
