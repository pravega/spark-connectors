<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

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
|---------------|-----------------|-----------------------------------------------------------------------------------|  

### Build and Install the Spark Connector

This will build the Spark Connector and publish it to your local Maven repository.

```
cd
git clone https://github.com/pravega/spark-connectors
cd spark-connectors
./gradlew install
ls -lhR ~/.m2/repository/io/pravega/pravega-connectors-spark
```

## Documentation

To learn more about how to build and use the Spark Connector library, refer to
[Pravega Samples](https://github.com/claudiofahey/pravega-samples/tree/spark-connector-examples/spark-connector-examples).

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
