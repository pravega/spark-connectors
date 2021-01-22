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

  - A Spark V2 data source micro-batch reader connector allows Spark Streaming applications to read Pravega Streams.
    Pravega stream cuts are used to reliably recover from failures and provide exactly-once semantics.
    
  - A Spark base relation data source batch reader connector allows Spark batch applications to read Pravega Streams.

  - A Spark V2 data source stream writer allows Spark Streaming applications to write to Pravega Streams.
    Writes are contained within Pravega transactions, providing exactly-once semantics.

  - Seamless integration with Spark's checkpoints.

  - Parallel Readers and Writers supporting high throughput and low latency processing.

  - Reader supports reassembling chunked events to support events of 2 GiB.

| Spark Version | Pravega Version | Java Version To Build Connector | Java Version To Run Connector | Git Branch                                                                        |
|---------------|-----------------|---------------------------------|-------------------------------|-----------------------------------------------------------------------------------|
| 3.0           | 0.9             | Java 11                         | Java 8 or 11                  | [master](https://github.com/pravega/spark-connectors)                             |
| 2.4           | 0.9             | Java 8                          | Java 8                        | [r0.9-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.9-spark2.4)   |
| 3.0           | 0.8             | Java 8                          | Java 8                        | [r0.8-spark3.0](https://github.com/pravega/spark-connectors/tree/r0.8-spark3.0)   |
| 2.4           | 0.8             | Java 8                          | Java 8                        | [r0.8-spark2.4](https://github.com/pravega/spark-connectors/tree/r0.8-spark2.4)   |
- Java 8 or 11 (see compatibility matrix)
## Limitations

  - The current implementation of this connector does *not* guarantee that events with the same routing key
    are returned in a single partition. 
    If your application requires this, you must repartition the dataframe by the routing key and sort within the
    partition by segment_id and offset.

  - Continuous reader support is not available. The micro-batch reader uses the Pravega batch API and works well for
    applications with latency requirements above 100 milliseconds.

  - The initial batch in the micro-batch reader will contain the entire Pravega stream as of the start time.
    There is no rate limiting functionality.

  - Read-after-write consistency is currently *not* guaranteed.
    Be cautious if your workflow requires multiple chained Spark batch jobs.

### Build and Install the Spark Connector

This will build the Spark Connector and publish it to your local Maven repository.

```
cd
git clone https://github.com/pravega/spark-connectors
cd spark-connectors
./gradlew install
ls -lhR ~/.m2/repository/io/pravega/pravega-connectors-spark
```

### Test the Spark Connector
As Pravega 0.9 runs on Java 11+ while spark 2.4 runs on Java 8, it needs to connect to a standalone Pravega instance for the test.
1. Start Pravega 0.9 using Java 11.
    1. Checkout the latest source code from [Pravage branch r0.9](https://github.com/pravega/pravega/tree/r0.9).
       ```
       git clone https://github.com/pravega/pravega.git
       cd pravega
       git checkout r0.9 
       ```
    2. Start Pravega standalone instance using Java 11.
       ```
       JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 ./gradlew startStandalone
       ```

2. Run spark-connectors/gradlew test using Java 8. 
   By default, the test will connect to Pravega instance listening on tcp://localhost:9090.
    ```
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 ./gradlew clean build
    ```
   In case to connect the Pravega instance listening on some other port or remote server for the test, just specify the pravega.uri value accordingly.
    ```
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 ./gradlew clean build -Dpravega.uri=tcp://server:port
    ```

## Documentation

To learn more about how to build and use the Spark Connector library, refer to
[Pravega Samples](https://github.com/claudiofahey/pravega-samples/tree/spark-connector-examples/spark-connector-examples).

## About

Spark connectors for Pravega is 100% open source and community-driven. All components are available
under [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0.html) on GitHub.

## Reference

- http://blog.madhukaraphatak.com/spark-datasource-v2-part-1/
