<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Pravega Spark Connectors [![Build Status](https://travis-ci.org/pravega/spark-connectors.svg?branch=master)](https://travis-ci.org/pravega/spark-connectors) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

This repository implements connectors to read and write [Pravega](http://pravega.io/) Streams with [Apache Spark](http://spark.apache.org/), a high-performance analytics engine for batch and streaming data.

Build end-to-end stream processing and batch pipelines that use Pravega as the stream storage and message bus, and Apache Spark for computation over the streams.

Pravega is an open source distributed storage service implementing **Streams**. It offers Stream as the main primitive for the foundation of reliable storage systems: a *high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency*.

To learn more about Pravega, visit https://pravega.io

## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**
  - A Spark micro-batch reader connector allows Spark streaming applications to read Pravega Streams.
    Pravega stream cuts (i.e. offsets) are used to reliably recover from failures and provide exactly-once semantics.
  - A Spark batch reader connector allows Spark batch applications to read Pravega Streams.
  - A Spark writer allows Spark batch and streaming applications to write to Pravega Streams.
    Writes are optionally contained within Pravega transactions, providing exactly-once semantics.
  - Seamless integration with Spark's checkpoints.
  - Parallel Readers and Writers supporting high throughput and low latency processing.

## Documentation

- [Spark Connectors for Pravega](documentation/src/docs/overview.md)

## Compatibility Matrix

The [master](https://github.com/pravega/spark-connectors) branch will always have the most recent supported versions of Spark and Pravega.

| Spark Version | Pravega Version | Java Version To Build Connector | Java Version To Run Connector | Git Branch                                                                        |
|---------------|-----------------|---------------------------------|-------------------------------|-----------------------------------------------------------------------------------|
| 3.3           | 0.12            | Java 11                         | Java 8 or 11                  | [master](https://github.com/pravega/spark-connectors)                             |
| 3.2           | 0.11            | Java 11                         | Java 8 or 11                  | [r0.11](https://github.com/pravega/spark-connectors/tree/r0.11)                   |
| 2.4           | 0.11            | Java 8                          | Java 8                        | [r0.11-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.11-spark2.4) |
| 3.1           | 0.10            | Java 11                         | Java 8 or 11                  | [r0.10](https://github.com/pravega/spark-connectors/tree/r0.10)                   |
| 3.0           | 0.10            | Java 11                         | Java 8 or 11                  | [r0.10-spark3.0](https://github.com/pravega/spark-connectors/tree/r0.10-spark3.0) |
| 2.4           | 0.10            | Java 8                          | Java 8                        | [r0.10-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.10-spark2.4) |

## Support

Don’t hesitate to ask! Contact the developers and community on [Slack](https://pravega-io.slack.com/) ([signup](https://pravega-slack-invite.herokuapp.com/)) if you need any help. Open an issue if you found a bug on [Github Issues](https://github.com/pravega/spark-connectors/issues).

## About

Spark Connectors for Pravega is 100% open source and community-driven. All components are available under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.
