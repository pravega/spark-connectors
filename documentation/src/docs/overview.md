---
title: Spark Connectors for Pravega
sidebar_label: Overview
---

<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

This documentation describes the connector API and usage to read and write [Pravega](http://pravega.io/) streams with [Apache Spark](http://spark.apache.org/).

Build end-to-end stream processing and batch pipelines that use Pravega as the stream storage and message bus, and Apache Spark for computation over the streams.

- [Getting Started](getting-started.md)
- [Samples](samples.md)
- [Configuration](configuration.md)
- [Compatibility Matrix](https://github.com/pravega/spark-connectors#compatibility-matrix)
- [Building the Connector](build-connector.md)
- [Features & Highlights](#features--highlights)
- [Limitations](limitations.md)
- [Releases](#releases)
- [Pre-Built Artifacts](#pre-built-artifacts)
- [Learn More](learn-more.md)
- [Support](#support)
- [About](#about)

## Features & Highlights

  - **Exactly-once processing guarantees** for both Reader and Writer, supporting **end-to-end exactly-once processing pipelines**
  - A Spark micro-batch reader connector allows Spark streaming applications to read Pravega Streams.
    Pravega stream cuts (i.e. offsets) are used to reliably recover from failures and provide exactly-once semantics.
  - A Spark batch reader connector allows Spark batch applications to read Pravega Streams.
  - A Spark writer allows Spark batch and streaming applications to write to Pravega Streams.
    Writes are optionally contained within Pravega transactions, providing exactly-once semantics.
  - Seamless integration with Spark's checkpoints.
  - Parallel Readers and Writers supporting high throughput and low latency processing.

## Releases

The latest releases can be found on the [Github Release](https://github.com/pravega/spark-connectors/releases) project page.

## Pre-Built Artifacts

Releases are published to Maven Central. Spark and Gradle will automatically download the required artifacts. However, if you wish, you may download the artifacts manually using the links below.

The pre-built artifacts are available in the following locations:

-  Maven Central (releases)
   -  [pravega-connectors-spark-3.1](https://mvnrepository.com/artifact/io.pravega/pravega-connectors-spark-3.1)
   -  [pravega-connectors-spark-2.4](https://mvnrepository.com/artifact/io.pravega/pravega-connectors-spark-2.4)
-  GitHub Packages (snapshots)
   -  [GitHub Packages](https://github.com/orgs/pravega/packages?repo_name=spark-connectors)

## Support

Don’t hesitate to ask! Contact the developers and community on [Slack](https://pravega-io.slack.com/) ([signup](https://pravega-slack-invite.herokuapp.com/)) if you need any help. Open an issue if you found a bug on [Github Issues](https://github.com/pravega/spark-connectors/issues).

## About

Spark Connectors for Pravega is 100% open source and community-driven. All components are available under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.
